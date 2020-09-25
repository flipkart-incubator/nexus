package store

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/smira/go-statsd"
)

type redisStore struct {
	cli   *redis.Client
	stats *statsd.Client
}

func (this *redisStore) incrStat(stat string, count int64, tags ...statsd.Tag) {
	if this.stats != nil {
		this.stats.Incr(stat, count, tags...)
	}
}

func (this *redisStore) endTimeStat(stat string, startTime int64, tags ...statsd.Tag) {
	if this.stats != nil {
		endTime := time.Now().UnixNano() / 1e6
		this.stats.Timing(stat, endTime-startTime, tags...)
	}
}

func (this *redisStore) startTimeStat() int64 {
	if this.stats != nil {
		return time.Now().UnixNano() / 1e6
	}
	return 0
}

func (this *redisStore) closeStats() {
	if this.stats != nil {
		this.stats.Close()
	}
}

func (this *redisStore) Close() error {
	this.closeStats()
	return this.cli.Close()
}

func isRedisError(err error) bool {
	return err != nil && !strings.HasSuffix(strings.TrimSpace(err.Error()), "nil")
}

func (this *redisStore) Load(data []byte) ([]byte, error) {
	defer this.endTimeStat("redis_load.latency.ms", this.startTimeStat())
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) Save(data []byte) ([]byte, error) {
	defer this.endTimeStat("redis_save.latency.ms", this.startTimeStat())
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) evalLua(luaScript string) ([]byte, error) {
	if res, err := this.cli.Eval(luaScript, nil).Result(); isRedisError(err) {
		this.incrStat("eval.lua.error", 1)
		return nil, err
	} else {
		if res == nil {
			return nil, nil
		} else {
			resStr := fmt.Sprintf("%v", res)
			return []byte(resStr), nil
		}
	}
}

func (this *redisStore) extractAllData() ([]map[string][]byte, error) {
	maxDBs := this.getMaxDBIdx()
	result := make([]map[string][]byte, maxDBs)
	for db := 0; db < maxDBs; db++ {
		cli := selectDB(db, this.cli)
		cursor := uint64(0)
		result[db] = make(map[string][]byte)

		for {
			keys, new_cursor, err := cli.Scan(cursor, "", 1000).Result()
			if isRedisError(err) {
				return nil, err
			}
			for _, key := range keys {
				if key_data, err := cli.Dump(key).Result(); isRedisError(err) {
					return nil, err
				} else {
					result[db][key] = []byte(key_data)
				}
			}
			if new_cursor == 0 {
				break
			} else {
				cursor = new_cursor
			}
		}
	}

	return result, nil
}

func (this *redisStore) loadAllData(redis_data_set []map[string][]byte, replaceable bool) error {
	for dbIdx, redis_data := range redis_data_set {
		cli := selectDB(dbIdx, this.cli)
		for k, v := range redis_data {
			if replaceable {
				if err := cli.RestoreReplace(k, 0, string(v)).Err(); isRedisError(err) {
					return err
				}
			} else {
				if err := cli.Del(k).Err(); isRedisError(err) {
					return err
				}
				if err := cli.Restore(k, 0, string(v)).Err(); isRedisError(err) {
					return err
				}
			}
		}
	}
	return nil
}

func (this *redisStore) Backup() ([]byte, error) {
	defer this.endTimeStat("redis_backup.latency.ms", this.startTimeStat())
	if data, err := this.extractAllData(); isRedisError(err) {
		this.incrStat("backup.extract.error", 1)
		return nil, err
	} else {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(data); err != nil {
			this.incrStat("backup.encode.error", 1)
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

func (this *redisStore) Restore(data []byte) error {
	defer this.endTimeStat("redis_restore.latency.ms", this.startTimeStat())
	var redis_data []map[string][]byte
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&redis_data); err != nil {
		this.incrStat("restore.decode.error", 1)
		return err
	}
	if err := this.loadAllData(redis_data, this.restoreReplaceSupported()); isRedisError(err) {
		this.incrStat("restore.load.error", 1)
		return err
	}
	return nil
}

func (this *redisStore) restoreReplaceSupported() bool {
	infoRes := this.cli.Info("server").Val()
	scanner := bufio.NewScanner(strings.NewReader(infoRes))
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		txt := scanner.Text()
		if strings.HasPrefix(txt, "redis_version") {
			verStr := strings.Split(txt, ":")[1]
			verStr = strings.Split(verStr, ".")[0]
			ver, _ := strconv.ParseFloat(verStr, 64)
			return ver >= 3
		}
	}
	return false
}

func selectDB(idx int, client *redis.Client) *redis.Client {
	opts := client.Options()
	newOpts := *opts
	newOpts.DB = idx
	return redis.NewClient(&newOpts)
}

func (this *redisStore) getMaxDBIdx() int {
	res, _ := this.cli.ConfigGet("databases").Result()
	dbIdxStr := res[1].(string)
	dbIdx, _ := strconv.Atoi(dbIdxStr)
	return dbIdx
}

func connect(redis_host string, redis_port uint) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:               fmt.Sprintf("%s:%d", redis_host, redis_port),
		Password:           "",
		IdleCheckFrequency: -1, // This value causes the Redis idle connection reaper go-routine to be disabled, since its impl. has a flaw that allows it to linger even after the connection is closed
	})
	_, err := client.Ping().Result()
	return client, err
}

func NewRedisDB(host string, port uint, stats *statsd.Client) (*redisStore, error) {
	if cli, err := connect(host, port); err != nil {
		return nil, err
	} else {
		return &redisStore{cli, stats}, nil
	}
}

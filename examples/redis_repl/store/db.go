package store

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/flipkart-incubator/nexus/internal/stats"
	"github.com/go-redis/redis"
)

type redisStore struct {
	cli      *redis.Client
	statsCli stats.Client
}

func (this *redisStore) Close() error {
	this.statsCli.Close()
	return this.cli.Close()
}

func isRedisError(err error) bool {
	return err != nil && !strings.HasSuffix(strings.TrimSpace(err.Error()), "nil")
}

func (this *redisStore) Load(data []byte) ([]byte, error) {
	defer this.statsCli.Timing("redis_load.latency.ms", time.Now())
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) Save(data []byte) ([]byte, error) {
	defer this.statsCli.Timing("redis_save.latency.ms", time.Now())
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) evalLua(luaScript string) ([]byte, error) {
	if res, err := this.cli.Eval(luaScript, nil).Result(); isRedisError(err) {
		this.statsCli.Incr("eval.lua.error", 1)
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
	defer this.statsCli.Timing("redis_backup.latency.ms", time.Now())
	if data, err := this.extractAllData(); isRedisError(err) {
		this.statsCli.Incr("backup.extract.error", 1)
		return nil, err
	} else {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(data); err != nil {
			this.statsCli.Incr("backup.encode.error", 1)
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

func (this *redisStore) Restore(data []byte) error {
	defer this.statsCli.Timing("redis_restore.latency.ms", time.Now())
	var redis_data []map[string][]byte
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&redis_data); err != nil {
		this.statsCli.Incr("restore.decode.error", 1)
		return err
	}
	if err := this.loadAllData(redis_data, this.restoreReplaceSupported()); isRedisError(err) {
		this.statsCli.Incr("restore.load.error", 1)
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

func NewRedisDB(host string, port uint, statsCli stats.Client) (*redisStore, error) {
	if cli, err := connect(host, port); err != nil {
		return nil, err
	} else {
		return &redisStore{cli, statsCli}, nil
	}
}

package store

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

type redisStore struct {
	cli *redis.Client
}

func (this *redisStore) Close() error {
	return this.cli.Close()
}

func isRedisError(err error) bool {
	return err != nil && !strings.HasSuffix(strings.TrimSpace(err.Error()), "nil")
}

func (this *redisStore) Load(data []byte) ([]byte, error) {
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) Save(data []byte) ([]byte, error) {
	luaScript := string(data)
	return this.evalLua(luaScript)
}

func (this *redisStore) evalLua(luaScript string) ([]byte, error) {
	if res, err := this.cli.Eval(luaScript, nil).Result(); isRedisError(err) {
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

const REDIS_MAX_DB_COUNT = 16

func (this *redisStore) extractAllData() ([]map[string][]byte, error) {
	result := make([]map[string][]byte, REDIS_MAX_DB_COUNT)
	for db := 0; db < REDIS_MAX_DB_COUNT; db++ {
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
				if err := cli.Del(k).Err(); err != nil {
					return err
				}
				if err := cli.Restore(k, 0, string(v)).Err(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (this *redisStore) Backup() ([]byte, error) {
	if data, err := this.extractAllData(); isRedisError(err) {
		return nil, err
	} else {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(data); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

func (this *redisStore) Restore(data []byte) error {
	var redis_data []map[string][]byte
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&redis_data); err != nil {
		return err
	}
	if err := this.loadAllData(redis_data, this.restoreReplaceSupported()); isRedisError(err) {
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
	opts.DB = idx
	return redis.NewClient(opts)
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

func NewRedisDB(host string, port uint) (*redisStore, error) {
	if cli, err := connect(host, port); err != nil {
		return nil, err
	} else {
		return &redisStore{cli}, nil
	}
}

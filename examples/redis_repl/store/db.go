package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

func (this *redisStore) extractAllData() (map[string][]byte, error) {
	redis_data := make(map[string][]byte)
	cursor := uint64(0)

	for {
		keys, new_cursor, err := this.cli.Scan(cursor, "", 1000).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if key_data, err := this.cli.Dump(key).Result(); err != nil {
				return nil, err
			} else {
				redis_data[key] = []byte(key_data)
			}
		}
		if new_cursor == 0 {
			break
		} else {
			cursor = new_cursor
		}
	}

	return redis_data, nil
}

func (this *redisStore) loadAllData(redis_data map[string][]byte) error {
	for k, v := range redis_data {
		if _, err := this.cli.RestoreReplace(k, 0, string(v)).Result(); err != nil {
			return err
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
	redis_data := make(map[string][]byte)
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&redis_data); err != nil {
		return err
	}
	if err := this.loadAllData(redis_data); isRedisError(err) {
		return err
	}
	return nil
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

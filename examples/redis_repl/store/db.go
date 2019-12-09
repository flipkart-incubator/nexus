package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
)

const LocalRedisHost = "127.0.0.1"

type redisStore struct {
	cli *redis.Client
}

type SaveRequest struct {
	Command string
	Key     string
	Val     interface{}
}

func (this *SaveRequest) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (this *SaveRequest) String() string {
	return fmt.Sprintf("%s->%s:%s", this.Command, this.Key, this.Val)
}

func (this *redisStore) Close() error {
	return this.cli.Close()
}

func (this *redisStore) Save(data []byte) error {
	save_req := SaveRequest{}
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&save_req); err != nil {
		return err
	} else {
		return this.save(&save_req)
	}
}

func (this *redisStore) save(save_req *SaveRequest) error {
	cmd := strings.ToLower(strings.TrimSpace(save_req.Command))
	switch cmd {
	case "set":
		return this.set(save_req.Key, save_req.Val)
	case "del":
		return this.del(save_req.Key)
	default:
		return errors.New(fmt.Sprintf("Unknown command: '%s'", save_req.Command))
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
	if data, err := this.extractAllData(); err != nil {
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
	return this.loadAllData(redis_data)
}

func (this *redisStore) set(key string, val interface{}) error {
	_, err := this.cli.Set(key, val, 0).Result()
	return err
}

func (this *redisStore) del(key string) error {
	_, err := this.cli.Del(key).Result()
	return err
}

func connect(redis_host string, redis_port, db_index int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:               fmt.Sprintf("%s:%d", redis_host, redis_port),
		Password:           "",
		DB:                 db_index,
		IdleCheckFrequency: -1, // This value causes the Redis idle connection reaper go-routine to be disabled, since its impl. has a flaw that allows it to linger even after the connection is closed
	})
	_, err := client.Ping().Result()
	return client, err
}

func NewRedisDB(port, db int) (*redisStore, error) {
	if port <= 0 || db < 0 || db > 15 {
		return nil, errors.New("A valid Redis port and DB index [0-15] must be given")
	}

	if cli, err := connect(LocalRedisHost, port, db); err != nil {
		return nil, err
	} else {
		return &redisStore{cli}, nil
	}
}

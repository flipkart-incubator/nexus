package store

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/flipkart-incubator/nexus/internal/stats"
	"github.com/go-redis/redis"
)

type redisStore struct {
	cli      *redis.Client
	statsCli stats.Client
	metaDB   uint
}

func (this *redisStore) Close() error {
	_ = this.statsCli.Close()
	return this.cli.Close()
}

func isRedisError(err error) bool {
	return err != nil && !strings.HasSuffix(strings.TrimSpace(err.Error()), "nil")
}

const (
	RaftStateKey      = "raft.state"
	RaftStateTermKey  = "term"
	RaftStateIndexKey = "index"
	DBIndexKey        = "db.index"
)

func (this *redisStore) GetLastAppliedEntry() (db.RaftEntry, error) {
	metaCli := selectDB(int(this.metaDB), this.cli)
	if result, err := metaCli.HGetAll(RaftStateKey).Result(); err != nil {
		return db.RaftEntry{}, err
	} else {
		term, _ := strconv.ParseUint(result[RaftStateTermKey], 10, 64)
		index, _ := strconv.ParseUint(result[RaftStateIndexKey], 10, 64)
		return db.RaftEntry{Term: term, Index: index}, nil
	}
}

const (
	LoadLUAScript = `
redis.call('select', '%d')
%s
`
	SaveLUAScript = `
redis.call('select', '%d')
redis.call('hset', '%s', '%s', '%d')
redis.call('hset', '%s', '%s', '%d')
redis.call('select', '%d')
%s
`
)

func (this *redisStore) Load(data []byte) ([]byte, error) {
	defer this.statsCli.Timing("redis_load.latency.ms", time.Now())

	req := new(api.LoadRequest)
	err := req.Decode(data)
	if err != nil {
		return nil, err
	}

	userDB, err := this.readUserDB(req.Args)
	if err != nil {
		return nil, err
	}

	luaSnippet := string(req.Data)
	luaScript := fmt.Sprintf(LoadLUAScript,
		userDB,			/* switch to user DB */
		luaSnippet)		/* user supplied Lua snippet */
	return this.evalLua(luaScript)
}

func (this *redisStore) Save(raftState db.RaftEntry, data []byte) ([]byte, error) {
	defer this.statsCli.Timing("redis_save.latency.ms", time.Now())

	req := new(api.SaveRequest)
	err := req.Decode(data)
	if err != nil {
		return nil, err
	}

	userDB, err := this.readUserDB(req.Args)
	if err != nil {
		return nil, err
	}

	luaSnippet := string(req.Data)
	luaScript := fmt.Sprintf(SaveLUAScript,
		this.metaDB,										/* switch to metadata DB */
		RaftStateKey, RaftStateTermKey, raftState.Term,		/* insert RAFT state term */
		RaftStateKey, RaftStateIndexKey, raftState.Index,	/* insert RAFT state index */
		userDB,												/* switch to user DB */
		luaSnippet)											/* user supplied Lua snippet */
	return this.evalLua(luaScript)
}

func (this *redisStore) readUserDB(args map[string][]byte) (int, error) {
	userDB := 0
	if dbIdx, present := args[DBIndexKey]; present {
		return strconv.Atoi(string(dbIdx))
	}
	return userDB, nil
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
	for dbIndex := 0; dbIndex < maxDBs; dbIndex++ {
		cli := selectDB(dbIndex, this.cli)
		cursor := uint64(0)
		result[dbIndex] = make(map[string][]byte)

		for {
			keys, new_cursor, err := cli.Scan(cursor, "", 1000).Result()
			if isRedisError(err) {
				return nil, err
			}
			for _, key := range keys {
				if key_data, err := cli.Dump(key).Result(); isRedisError(err) {
					return nil, err
				} else {
					result[dbIndex][key] = []byte(key_data)
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

func (this *redisStore) Backup(_ db.SnapshotState) (io.ReadCloser, error) {
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
		return ioutil.NopCloser(&buf), nil
	}
}

func (this *redisStore) Restore(data io.ReadCloser) error {
	defer this.statsCli.Timing("redis_restore.latency.ms", time.Now())
	defer data.Close()
	var redisData []map[string][]byte
	if err := gob.NewDecoder(data).Decode(&redisData); err != nil {
		this.statsCli.Incr("restore.decode.error", 1)
		return err
	}
	if err := this.loadAllData(redisData, this.restoreReplaceSupported()); isRedisError(err) {
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

func NewRedisDB(host string, port, metadataDB uint, statsCli stats.Client) (*redisStore, error) {
	if cli, err := connect(host, port); err != nil {
		return nil, err
	} else {
		return &redisStore{cli, statsCli, metadataDB}, nil
	}
}

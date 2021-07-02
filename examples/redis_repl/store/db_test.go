package store

import (
	"fmt"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"os"
	"testing"
	"time"

	"github.com/flipkart-incubator/nexus/internal/stats"
)

var store *redisStore

const (
	redisHost = "127.0.0.1"
	redisPort = 6379
	keyPref   = "NEXUS_TEST"
	value     = "hello_world"
	redisDB   = 2
	metadataDB = 13
)

func TestMain(m *testing.M) {
	initRedisStore()
	os.Exit(m.Run())
}

func initRedisStore() {
	if rs, err := NewRedisDB(redisHost, redisPort, metadataDB, stats.NewNoOpClient()); err != nil {
		panic(err)
	} else {
		store = rs
	}
}

func insertKey(t *testing.T, key, val string, dbIdx int) {
	saveReq := fmt.Sprintf("redis.call('select', '%d') return redis.call('set', '%s', '%s')", dbIdx, key, val)
	if saveRes, err := store.Save(db.RaftEntry{Term: 2, Index: 10}, []byte(saveReq)); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("Insert response received: %s", saveRes)
	}
}

func deleteKey(t *testing.T, key string, dbIdx int) {
	delReq := fmt.Sprintf("redis.call('select', '%d') return redis.call('del', '%s')", dbIdx, key)
	if delRes, err := store.Save(db.RaftEntry{}, []byte(delReq)); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("Delete response received: %s", delRes)
	}
}

func assertKey(t *testing.T, key, expVal string, dbIdx int) {
	loadReq := fmt.Sprintf("redis.call('select', '%d') return redis.call('get', '%s')", dbIdx, key)
	if loadRes, err := store.Save(db.RaftEntry{}, []byte(loadReq)); err != nil {
		t.Fatal(err)
	} else {
		if string(loadRes) != expVal {
			t.Errorf("Value mismatch. Expected: '%s' Actual: '%s' DB: %d, Key: %s", expVal, loadRes, dbIdx, key)
		} else {
			t.Logf("Load response received: %s", loadRes)
		}
	}
}

func TestRedisSaveLoad(t *testing.T) {
	currTime := time.Now().Unix()
	key := fmt.Sprintf("%s_%d", keyPref, currTime)

	// insert key
	insertKey(t, key, value, redisDB)

	// load key
	assertKey(t, key, value, redisDB)

	// delete key
	deleteKey(t, key, redisDB)
}

func TestBackupRestore(t *testing.T) {
	currTime := time.Now().Unix()
	numDBs, numKeys := 5, 10

	// insert 10 keys in each of the 5 DBs
	for db := 0; db < numDBs; db++ {
		for ki := 0; ki < numKeys; ki++ {
			key := fmt.Sprintf("%s_%d_%d", keyPref, ki, currTime)
			insertKey(t, key, value, db)
		}
	}

	// backup the DB
	bkp, err := store.Backup(db.SnapshotState{})
	if err != nil {
		t.Fatal(err)
	}

	// overwrite all keys from all DBs
	for db := 0; db < numDBs; db++ {
		for ki := 0; ki < numKeys; ki++ {
			key := fmt.Sprintf("%s_%d_%d", keyPref, ki, currTime)
			insertKey(t, key, "some_value", db)
			//deleteKey(t, key, db)
		}
	}

	// restore the DB
	err = store.Restore(bkp)
	if err != nil {
		t.Fatal(err)
	}

	// verify for the presence of keys
	for db := 0; db < numDBs; db++ {
		for ki := 0; ki < numKeys; ki++ {
			key := fmt.Sprintf("%s_%d_%d", keyPref, ki, currTime)
			assertKey(t, key, value, db)
		}
	}
}

func TestRestoreReplaceSupported(t *testing.T) {
	res := store.restoreReplaceSupported()
	t.Logf("Restore replace supported: %v", res)
}

func TestMaxDBIdx(t *testing.T) {
	dbIdx := store.getMaxDBIdx()
	t.Logf("Max DB index: %d", dbIdx)
}

func TestSelectDB(t *testing.T) {
	oldCli := store.cli
	newCli := selectDB(3, oldCli)

	t.Logf("Old client's selected DB: %d", oldCli.Options().DB)
	t.Logf("New client's selected DB: %d", newCli.Options().DB)
}

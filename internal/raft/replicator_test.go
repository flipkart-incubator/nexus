package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/flipkart-incubator/nexus/pkg/raft"
)

const (
	clusterSize = 3
	logDir      = "/tmp/nexus_test/logs"
	snapDir     = "/tmp/nexus_test/snap"
	clusterUrl  = "http://127.0.0.1:9321,http://127.0.0.1:9322,http://127.0.0.1:9323"
	peer4Id     = 4
	peer4Url    = "http://127.0.0.1:9324"
	replTimeout = 3 * time.Second
)

var clus *cluster

func TestReplicator(t *testing.T) {
	clus = startCluster(t)
	defer clus.stop()

	t.Run("testSaveLoadData", testSaveLoadData)
	t.Run("testLoadTimingIssues", testLoadTimingIssues)
	t.Run("testForNewNexusNodeJoinLeaveCluster", testForNewNexusNodeJoinLeaveCluster)
	t.Run("testForNodeRestart", testForNodeRestart)
}

func testSaveLoadData(t *testing.T) {
	var reqs []*kvReq
	// Saving
	for _, peer := range clus.peers {
		req1 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 1), fmt.Sprintf("Val:%d#%d", peer.id, 1)}
		peer.save(t, req1)
		reqs = append(reqs, req1)

		req2 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 2), fmt.Sprintf("Val:%d#%d", peer.id, 2)}
		peer.save(t, req2)
		reqs = append(reqs, req2)

		req3 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 3), fmt.Sprintf("Val:%d#%d", peer.id, 3)}
		peer.save(t, req3)
		reqs = append(reqs, req3)
	}

	//assertions
	clus.assertDB(t, reqs...)

	// Loading
	for _, req := range reqs {
		expVal := req.Val.(string)
		for _, peer := range clus.peers {
			actVal := peer.load(t, req).(string)
			if expVal != actVal {
				t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", peer.id, req.Key, expVal, actVal)
			}
		}
	}
}

func testLoadTimingIssues(t *testing.T) {
	peer1, peer2, peer3 := clus.peers[0], clus.peers[1], clus.peers[2]
	peer3.savePause(10 * time.Second)
	defer peer3.savePause(0)

	req := &kvReq{fmt.Sprintf("LoadKey:%d", peer1.id), fmt.Sprintf("LoadVal:%d", peer1.id)}
	peer1.save(t, req)

	peer1.assertDB(t, req)
	peer2.assertDB(t, req)
	//peer3.assertDB(t, req)

	peer3.expectLoadFailure(t, req)
}

func testForNewNexusNodeJoinLeaveCluster(t *testing.T) {
	peer1 := clus.peers[0]
	if err := peer1.repl.AddMember(context.Background(), peer4Id, peer4Url); err != nil {
		t.Fatal(err)
	}
	sleep(3)
	clusUrl := fmt.Sprintf("%s,%s", clusterUrl, peer4Url)
	if peer4, err := newJoiningPeer(peer4Id, clusUrl); err != nil {
		t.Fatal(err)
	} else {
		peer4.start()
		sleep(3)
		db4, db1 := peer4.db.content, peer1.db.content
		if !reflect.DeepEqual(db4, db1) {
			t.Errorf("DB Mismatch !!! Expected: %v, Actual: %v", db4, db1)
		}
		if err := peer1.repl.RemoveMember(context.Background(), peer4Id); err != nil {
			t.Fatal(err)
		}
		sleep(3)
		peer4.stop()
	}
}

func testForNodeRestart(t *testing.T) {
	peer2 := clus.peers[1]
	reqs := []*kvReq{&kvReq{"hello", "world"}, &kvReq{"foo", "bar"}}
	peer2.save(t, reqs[0])
	peer2.save(t, reqs[1])
	clus.assertDB(t, reqs...)

	peer2.stop()
	sleep(3)

	peer1 := clus.peers[0]
	new_reqs := []*kvReq{&kvReq{"micro", "soft"}, &kvReq{"wel", "come"}}
	peer1.save(t, new_reqs[0])
	peer1.save(t, new_reqs[1])

	var err error
	peer2, err = newPeerWithDB(2, peer2.db)
	if err != nil {
		t.Fatal(err)
	}
	clus.peers[1] = peer2
	peer2.start()
	sleep(3)
	clus.assertDB(t, new_reqs...)
}

func startCluster(t *testing.T) *cluster {
	if cl, err := newCluster(clusterSize); err != nil {
		t.Fatal(err)
		return nil
	} else {
		cl.start()
		return cl
	}
}

type cluster struct {
	peers []*peer
}

func newCluster(size int) (*cluster, error) {
	if size < 3 {
		return nil, errors.New(fmt.Sprintf("Given size: %d. Minimum size must be 3", size))
	}

	if err := createRaftDirs(); err != nil {
		return nil, err
	}

	peers := make([]*peer, size)
	for i := 1; i <= size; i++ {
		if peer, err := newPeer(i); err != nil {
			return nil, err
		} else {
			peers[i-1] = peer
		}
	}
	return &cluster{peers}, nil
}

func (this *cluster) start() {
	for _, peer := range this.peers {
		peer.start()
	}
	sleep(3)
}

func (this *cluster) stop() {
	for _, peer := range this.peers {
		peer.stop()
	}
}

func (this *cluster) assertDB(t *testing.T, reqs ...*kvReq) {
	for _, peer := range this.peers {
		peer.assertDB(t, reqs...)
	}
}

type peer struct {
	id   int
	db   *inMemKVStore
	repl *replicator
}

func newPeerWithDB(id int, db *inMemKVStore) (*peer, error) {
	opts, err := raft.NewOptions(
		raft.NodeId(id),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
	)
	if err != nil {
		return nil, err
	} else {
		repl := NewReplicator(db, opts)
		return &peer{id, db, repl}, nil
	}
}

func newPeer(id int) (*peer, error) {
	db := newInMemKVStore()
	return newPeerWithDB(id, db)
}

func newJoiningPeer(id int, clusUrl string) (*peer, error) {
	opts, err := raft.NewOptions(
		raft.NodeId(id),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.Join(true), // `true` since this node is joining an existing cluster
	)
	if err != nil {
		return nil, err
	} else {
		db := newInMemKVStore()
		repl := NewReplicator(db, opts)
		return &peer{id, db, repl}, nil
	}
}

func (this *peer) start() {
	this.repl.Start()
}

func (this *peer) stop() {
	this.repl.Stop()
}

func (this *peer) loadPause(pauseTime time.Duration) {
	this.db.pauseInLoadTime = pauseTime
}

func (this *peer) savePause(pauseTime time.Duration) {
	this.db.pauseInSaveTime = pauseTime
}

func (this *peer) save(t *testing.T, req *kvReq) {
	if bts, err := req.toBytes(); err != nil {
		t.Fatal(err)
	} else {
		if _, err := this.repl.Save(context.Background(), bts); err != nil {
			t.Fatal(err)
		} else {
			sleep(1)
		}
	}
}

func (this *peer) expectLoadFailure(t *testing.T, req *kvReq) interface{} {
	if bts, err := req.toBytes(); err != nil {
		t.Fatal(err)
	} else {
		if _, err := this.repl.Load(context.Background(), bts); err != nil {
			t.Log(err)
		} else {
			t.Errorf("Expected failure but got no error")
		}
	}
	return nil
}

func (this *peer) load(t *testing.T, req *kvReq) interface{} {
	if bts, err := req.toBytes(); err != nil {
		t.Fatal(err)
	} else {
		if kvBts, err := this.repl.Load(context.Background(), bts); err != nil {
			t.Fatal(err)
		} else {
			if kv, err := fromBytes(kvBts); err != nil {
				t.Fatal(err)
			} else {
				return kv.Val
			}
		}
	}
	return nil
}

func (this *peer) assertDB(t *testing.T, reqs ...*kvReq) {
	for _, req := range reqs {
		if data, present := this.db.content[req.Key]; !present {
			t.Errorf("peer %d -> Expected key: %s to be present. But not found.", this.id, req.Key)
		} else if !reflect.DeepEqual(data, req.Val) {
			t.Errorf("peer %d -> Value mismatch !!! Expected: %q, Actual: %q", this.id, req.Val, data)
		}
	}
}

type kvReq struct {
	Key string
	Val interface{}
}

func fromBytes(data []byte) (*kvReq, error) {
	save_req := kvReq{}
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&save_req); err != nil {
		return nil, err
	} else {
		return &save_req, nil
	}
}

func (this *kvReq) toBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type inMemKVStore struct {
	mu      sync.Mutex
	content map[string]interface{}

	// used for pausing the peer for simulating timing effects
	pauseInSaveTime, pauseInLoadTime time.Duration
}

func newInMemKVStore() *inMemKVStore {
	return &inMemKVStore{content: make(map[string]interface{})}
}

func (this *inMemKVStore) Close() error {
	return nil
}

func (this *inMemKVStore) Load(data []byte) ([]byte, error) {
	<-time.After(this.pauseInLoadTime)
	this.mu.Lock()
	defer this.mu.Unlock()
	if kvReq, err := fromBytes(data); err != nil {
		return nil, err
	} else {
		if val, present := this.content[kvReq.Key]; present {
			kvReq.Val = val
			return kvReq.toBytes()
		} else {
			return nil, errors.New("not found")
		}
	}
}

func (this *inMemKVStore) Save(data []byte) ([]byte, error) {
	<-time.After(this.pauseInSaveTime)
	this.mu.Lock()
	defer this.mu.Unlock()
	if kvReq, err := fromBytes(data); err != nil {
		return nil, err
	} else {
		key := kvReq.Key
		if _, present := this.content[key]; present {
			return nil, errors.New(fmt.Sprintf("Given key: %s already exists", key))
		} else {
			this.content[key] = kvReq.Val
			return nil, nil
		}
	}
}

func (this *inMemKVStore) Backup() ([]byte, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this.content); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (this *inMemKVStore) Restore(data []byte) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	content := make(map[string]interface{})
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&content); err != nil {
		return err
	} else {
		this.content = content
		return nil
	}
}

func createRaftDirs() error {
	if err := os.RemoveAll(snapDir); err != nil {
		return err
	}
	if err := os.RemoveAll(logDir); err != nil {
		return err
	}
	if err := os.MkdirAll(snapDir, os.ModeDir|0777); err != nil {
		return err
	}
	if err := os.MkdirAll(logDir, os.ModeDir|0777); err != nil {
		return err
	}
	return nil
}

func sleep(durationInSecs int) {
	<-time.After(time.Duration(durationInSecs) * time.Second)
}

package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
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

	t.Run("testSaveData", testSaveData)
	t.Run("testForNewNexusNodeJoiningCluster", testForNewNexusNodeJoiningCluster)
	t.Run("testForNodeRestart", testForNodeRestart)
}

func testSaveData(t *testing.T) {
	var reqs []*kvReq
	for _, peer := range clus.peers {
		req1 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 1), time.Now().Unix()}
		peer.replicate(t, req1)
		reqs = append(reqs, req1)

		req2 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 2), time.Now().Unix()}
		peer.replicate(t, req2)
		reqs = append(reqs, req2)

		req3 := &kvReq{fmt.Sprintf("Key:%d#%d", peer.id, 3), time.Now().Unix()}
		peer.replicate(t, req3)
		reqs = append(reqs, req3)
	}
	clus.assertDB(t, reqs...)
}

func testForNewNexusNodeJoiningCluster(t *testing.T) {
	peer1 := clus.peers[0]
	clusUrl := fmt.Sprintf("%s,%s", clusterUrl, peer4Url)
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: uint64(peer4Id), Context: []byte(peer4Url)}
	peer1.repl.ProposeConfigChange(cc)
	sleep(3)
	if peer4, err := newJoiningPeer(peer4Id, clusUrl); err != nil {
		t.Fatal(err)
	} else {
		peer4.start()
		sleep(3)
		clus.peers = append(clus.peers, peer4)
		db4, db1 := peer4.db.content, peer1.db.content
		if !reflect.DeepEqual(db4, db1) {
			t.Errorf("DB Mismatch !!! Expected: %v, Actual: %v", db4, db1)
		}
	}
}

func testForNodeRestart(t *testing.T) {
	peer2 := clus.peers[1]
	reqs := []*kvReq{&kvReq{"hello", "world"}, &kvReq{"foo", "bar"}}
	peer2.replicate(t, reqs[0])
	peer2.replicate(t, reqs[1])
	clus.assertDB(t, reqs...)

	peer2.stop()
	sleep(3)

	peer1 := clus.peers[0]
	new_reqs := []*kvReq{&kvReq{"micro", "soft"}, &kvReq{"wel", "come"}}
	peer1.replicate(t, new_reqs[0])
	peer1.replicate(t, new_reqs[1])

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
	repl, err := NewReplicator(db,
		raft.NodeId(id),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
	)
	if err != nil {
		return nil, err
	} else {
		return &peer{id, db, repl}, nil
	}
}

func newPeer(id int) (*peer, error) {
	db := newInMemKVStore()
	return newPeerWithDB(id, db)
}

func newJoiningPeer(id int, clusUrl string) (*peer, error) {
	db := newInMemKVStore()
	repl, err := NewReplicator(db,
		raft.NodeId(id),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.Join(true),
	)
	if err != nil {
		return nil, err
	} else {
		return &peer{id, db, repl}, nil
	}
}

func (this *peer) start() {
	this.repl.Start()
}

func (this *peer) stop() {
	this.repl.Stop()
}

func (this *peer) replicate(t *testing.T, req *kvReq) {
	if bts, err := req.toBytes(); err != nil {
		t.Fatal(err)
	} else {
		if err := this.repl.Replicate(context.Background(), bts); err != nil {
			t.Fatal(err)
		} else {
			sleep(1)
		}
	}
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
	content map[string]interface{}
}

func newInMemKVStore() *inMemKVStore {
	return &inMemKVStore{content: make(map[string]interface{})}
}

func (this *inMemKVStore) Close() error {
	return nil
}

func (this *inMemKVStore) Save(data []byte) error {
	if kvReq, err := fromBytes(data); err != nil {
		return err
	} else {
		key := kvReq.Key
		if _, present := this.content[key]; present {
			return errors.New(fmt.Sprintf("Given key: %s already exists", key))
		} else {
			this.content[key] = kvReq.Val
			return nil
		}
	}
}

func (this *inMemKVStore) Backup() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this.content); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (this *inMemKVStore) Restore(data []byte) error {
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

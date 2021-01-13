package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
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
	peer4Url    = "http://127.0.0.1:9324"
	replTimeout = 3 * time.Second
)

var clus *cluster

func TestReplicator(t *testing.T) {
	clus = startCluster(t)
	defer clus.stop()

	t.Run("testListMembers", testListMembers)
	t.Run("testSaveLoadData", testSaveLoadData)
	t.Run("testLoadDuringRestarts", testLoadDuringRestarts)
	t.Run("testForNewNexusNodeJoinLeaveCluster", testForNewNexusNodeJoinLeaveCluster)
	t.Run("testForNodeRestart", testForNodeRestart)
}

func testListMembers(t *testing.T) {
	members := strings.Split(clusterUrl, ",")
	clus.assertMembers(t, members)
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

func testLoadDuringRestarts(t *testing.T) {
	peer1, peer2, peer3 := clus.peers[0], clus.peers[1], clus.peers[2]
	// stop peer3
	peer3.stop()
	sleep(3)

	// insert few KVs on the remaining cluster via peer1
	reqs := []*kvReq{
		&kvReq{
			fmt.Sprintf("LoadKey1:%d", peer1.id), fmt.Sprintf("LoadVal1:%d", peer1.id),
		},
		&kvReq{
			fmt.Sprintf("LoadKey2:%d", peer1.id), fmt.Sprintf("LoadVal2:%d", peer1.id),
		},
		&kvReq{
			fmt.Sprintf("LoadKey3:%d", peer1.id), fmt.Sprintf("LoadVal3:%d", peer1.id),
		},
	}
	peer1.save(t, reqs...)

	// assertions on peers
	peer1.assertDB(t, reqs...)
	peer2.assertDB(t, reqs...)
	peer3.expectMismatchDB(t, reqs...)

	// start peer3
	var err error
	peer3, err = newPeerWithDB(2, peer3.db)
	if err != nil {
		t.Fatal(err)
	}
	clus.peers[2] = peer3
	peer3.start()
	sleep(3)

	// load the entries on peer3 that were committed on other nodes
	actVal := peer3.load(t, reqs[0]).(string)
	if reqs[0].Val != actVal {
		t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", peer3.id, reqs[0].Key, reqs[0].Val, actVal)
	}
	actVal = peer3.load(t, reqs[1]).(string)
	if reqs[1].Val != actVal {
		t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", peer3.id, reqs[1].Key, reqs[1].Val, actVal)
	}
	actVal = peer3.load(t, reqs[2]).(string)
	if reqs[2].Val != actVal {
		t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", peer3.id, reqs[2].Key, reqs[2].Val, actVal)
	}

	// all assertions must work
	peer3.assertDB(t, reqs...)
}

func testForNewNexusNodeJoinLeaveCluster(t *testing.T) {
	// create a new peer
	if peer4, err := newJoiningPeer(peer4Url); err != nil {
		t.Fatal(err)
	} else {
		// start the peer
		peer4.start()
		sleep(3)
		peer1 := clus.peers[0]

		// add peer to existing cluster
		if err := peer1.repl.AddMember(context.Background(), peer4Url); err != nil {
			t.Fatal(err)
		}
		sleep(3)
		members := strings.Split(clusterUrl, ",")
		members = append(members, peer4Url)
		clus.assertMembers(t, members)

		// insert data
		db4, db1 := peer4.db.content, peer1.db.content
		if !reflect.DeepEqual(db4, db1) {
			t.Errorf("DB Mismatch !!! Expected: %v, Actual: %v", db4, db1)
		}

		// assert membership across all nodes
		peer4.assertMembers(t, peer4.getLeaderUrl(), members)

		// remove this peer
		if err := peer1.repl.RemoveMember(context.Background(), peer4Url); err != nil {
			t.Fatal(err)
		}
		sleep(3)

		// assert membership across all nodes
		clus.assertMembers(t, members[0:len(members)-1])
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
	peer2, err = newPeerWithDB(1, peer2.db)
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
		return nil, fmt.Errorf("Given size: %d. Minimum size must be 3", size)
	}

	if err := createRaftDirs(); err != nil {
		return nil, err
	}

	peers := make([]*peer, size)
	for i := 0; i < size; i++ {
		if peer, err := newPeer(i); err != nil {
			return nil, err
		} else {
			peers[i] = peer
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

func (this *cluster) assertMembers(t *testing.T, members []string) {
	lead := this.peers[0].getLeaderUrl()
	for _, peer := range this.peers {
		peer.assertMembers(t, lead, members)
	}
}

func (peer *peer) assertMembers(t *testing.T, leader string, members []string) {
	clusNodes := peer.repl.ListMembers()
	var clusMembers []string
	for _, clusNode := range clusNodes {
		if idx := strings.Index(clusNode, " (leader)"); idx > 0 {
			clusNode = clusNode[:idx]
			if clusNode != leader {
				t.Errorf("For peer ID: %v, mismatch of URL for leader. Expected: '%v', Actual: '%v'", peer.id, leader, clusNode)
			}
		}
		clusMembers = append(clusMembers, clusNode)
	}
	sort.Strings(clusMembers)
	sort.Strings(members)
	if !reflect.DeepEqual(clusMembers, members) {
		t.Errorf("For peer ID: %v, mismatch of members. Expected: %v, Actual: %v", peer.id, members, clusMembers)
	}
}

type peer struct {
	id   uint64
	db   *inMemKVStore
	repl *replicator
}

func newPeerWithDB(id int, db *inMemKVStore) (*peer, error) {
	peerAddr := strings.Split(clusterUrl, ",")[id]
	opts, err := raft.NewOptions(
		raft.ListenAddr(peerAddr),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.LeaseBasedReads(false),
	)
	if err != nil {
		return nil, err
	} else {
		repl := NewReplicator(db, opts)
		return &peer{repl.node.id, db, repl}, nil
	}
}

func newPeer(id int) (*peer, error) {
	db := newInMemKVStore()
	return newPeerWithDB(id, db)
}

func newJoiningPeer(peerAddr string) (*peer, error) {
	opts, err := raft.NewOptions(
		raft.ListenAddr(peerAddr),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.LeaseBasedReads(false),
	)
	if err != nil {
		return nil, err
	} else {
		db := newInMemKVStore()
		repl := NewReplicator(db, opts)
		return &peer{repl.node.id, db, repl}, nil
	}
}

func (this *peer) getLeaderUrl() string {
	lid := this.repl.node.getLeaderId()
	return this.repl.node.rpeers[lid]
}

func (this *peer) start() {
	this.repl.Start()
}

func (this *peer) stop() {
	this.repl.Stop()
}

func (this *peer) save(t *testing.T, reqs ...*kvReq) {
	for _, req := range reqs {
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

func (this *peer) expectMismatchDB(t *testing.T, reqs ...*kvReq) {
	mismatch := false
	for _, req := range reqs {
		if data, present := this.db.content[req.Key]; !present {
			mismatch = true
			break
		} else if !reflect.DeepEqual(data, req.Val) {
			mismatch = true
			break
		}
	}
	if !mismatch {
		t.Errorf("Expected mismatch of DB for peer: %d", this.id)
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
	mu      sync.Mutex
	content map[string]interface{}
}

func newInMemKVStore() *inMemKVStore {
	return &inMemKVStore{content: make(map[string]interface{})}
}

func (this *inMemKVStore) Close() error {
	return nil
}

func (this *inMemKVStore) Load(data []byte) ([]byte, error) {
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

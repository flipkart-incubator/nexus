package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/nexus/models"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
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
	peer5Url    = "http://127.0.0.1:9325"
	replTimeout = 3 * time.Second
)

var clus *cluster

func TestReplicator(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	clus = startCluster(t)
	defer clus.stop()

	t.Run("testListMembers", testListMembers)
	t.Run("testSaveLoadData", testSaveLoadData)
	t.Run("testSaveLoadLargeData", testSaveLoadLargeData)
	t.Run("testLoadDuringRestarts", testLoadDuringRestarts)
	t.Run("testForNewNexusNodeJoinLeaveCluster", testForNewNexusNodeJoinLeaveCluster)
	t.Run("testSaveLoadReallyLargeData", testSaveLoadReallyLargeData)
	t.Run("testForNewNexusNodeJoinHighDataClusterDataMismatch", testForNewNexusNodeJoinHighDataClusterDataMismatch)
	t.Run("testForNodeRestart", testForNodeRestart)
}

func testListMembers(t *testing.T) {
	members := strings.Split(clusterUrl, ",")
	clus.assertMembers(t, members)
	clus.assertRaftMembers(t)
}

func testSaveLoadReallyLargeData(t *testing.T) {
	var reqs []*kvReq
	iterations := 50
	// Saving
	writePeer := clus.peers[0]
	runId := writePeer.id
	token := make([]byte, 1024*1024*2) //1mb
	rand.Read(token)

	for i := 0; i < iterations; i++ {
		req1 := &kvReq{fmt.Sprintf("Key:KL%d#%d", runId, i), fmt.Sprintf("Val:%d#%d$%s", runId, i, token)}
		writePeer.save(t, req1)
		reqs = append(reqs, req1)
		t.Logf("Write KEY : %s", req1.Key)
	}

	//assertions
	clus.assertDB(t, reqs...)

	//Read
	for i := 0; i < iterations; i++ {
		req1 := &kvReq{fmt.Sprintf("Key:KL%d#%d", runId, i), fmt.Sprintf("Val:%d#%d$%s", runId, i, token)}
		actVal := writePeer.load(t, req1).(string)
		if req1.Val != actVal {
			t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", writePeer.id, req1.Key, req1.Val, actVal)
		}
	}
}

func testSaveLoadLargeData(t *testing.T) {
	var reqs []*kvReq
	iterations := 20
	// Saving
	writePeer := clus.peers[0]
	runId := writePeer.id

	for i := 0; i < iterations; i++ {
		req1 := &kvReq{fmt.Sprintf("Key:K%d#%d", runId, i), fmt.Sprintf("Val:%d#%d", runId, i)}
		writePeer.save(t, req1)
		reqs = append(reqs, req1)
		t.Logf("Write KEY : %s", req1.Key)
	}

	//assertions
	clus.assertDB(t, reqs...)

	//Read
	for i := 0; i < iterations; i++ {
		req1 := &kvReq{fmt.Sprintf("Key:K%d#%d", runId, i), fmt.Sprintf("Val:%d#%d", runId, i)}
		actVal := writePeer.load(t, req1).(string)
		if req1.Val != actVal {
			t.Errorf("Value mismatch for peer: %d. Key: %s, Expected Value: %s, Actual Value: %s", writePeer.id, req1.Key, req1.Val, actVal)
		}
	}
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
		if err = peer1.repl.AddMember(context.Background(), peer4Url); err != nil {
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

		//// remove this peer
		if err = peer1.repl.RemoveMember(context.Background(), peer4Url); err != nil {
			t.Fatal(err)
		}
		sleep(3)

		// assert membership across all nodes
		clus.assertMembers(t, members[0:len(members)-1])
		peer4.stop()
	}
}

func testForNewNexusNodeJoinHighDataClusterDataMismatch(t *testing.T) {
	// create a new peer
	if peer5, err := newJoiningPeer(peer5Url); err != nil {
		t.Fatal(err)
	} else {
		// start the peer
		peer5.start()
		sleep(5)
		peer1 := clus.peers[0]

		// add peer to existing cluster
		if err = peer1.repl.AddMember(context.Background(), peer5Url); err != nil {
			t.Fatal(err)
		}
		sleep(30)
		members := strings.Split(clusterUrl, ",")
		members = append(members, peer5Url)
		clus.assertMembers(t, members)

		//raft index
		if !reflect.DeepEqual(peer1.repl.node.appliedIndex, peer5.repl.node.appliedIndex) {
			t.Fatalf("Raft appliedIndex should match. Expectd %d Got %d", peer1.repl.node.appliedIndex, peer5.repl.node.appliedIndex)
		}

		db5, db1 := peer5.db.content, peer1.db.content

		//lets match the num keys count.
		if len(db5) != len(db1) {
			//find difference of keys
			keyDiff := difference(db1, db5)
			t.Fatalf("DB Mismatch Happened. Missing keys (%d) %+v !!!", len(keyDiff), keyDiff)
		}

		if !reflect.DeepEqual(db5, db1) {
			t.Fatal("DB Mismatch Happened. Not Expected !!!")
		}

		// assert membership across all nodes
		peer5.assertMembers(t, peer5.getLeaderUrl(), members)

		// remove this peer
		if err = peer1.repl.RemoveMember(context.Background(), peer5Url); err != nil {
			t.Fatal(err)
		}
		sleep(3)

		// assert membership across all nodes
		clus.assertMembers(t, members[0:len(members)-1])
		peer5.stop()
	}
}

// difference returns the elements in `a` that aren't in `b`.
func difference(a, b map[string]interface{}) []string {
	mb := make(map[string]struct{}, len(b))
	for x, _ := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for x, _ := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
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

func (this *cluster) assertRaftMembers(t *testing.T) {
	for _, peer := range this.peers {
		peer.assertRaftMembers(t)
	}
}

func (peer *peer) assertRaftMembers(t *testing.T) {
	leaderNode, clusNodeInfo := peer.repl.ListMembers()
	if leaderNode == 0 {
		t.Errorf("Leader node cannot be 0")
	}

	leaderCount := 0
	followerCount := 0
	for _, node := range clusNodeInfo {
		if node.Status == models.NodeInfo_LEADER {
			leaderCount++
		} else if node.Status == models.NodeInfo_FOLLOWER {
			followerCount++
		} else {
			t.Errorf("Incorrect node status. Actual %s", node.Status.String())
		}
	}
	if leaderCount != 1 {
		t.Errorf("Incorrect leader counts . Expected %d, Actual %d", 1, leaderCount)
	}
	expectedFollowerCount := len(clusNodeInfo) - 1
	if followerCount != expectedFollowerCount {
		t.Errorf("Incorrect follower counts . Expected %d, Actual %d", expectedFollowerCount, followerCount)
	}

}

func (peer *peer) assertMembers(t *testing.T, leader string, members []string) {
	leaderNode, clusNodes := peer.repl.ListMembers()
	if clusNodes[leaderNode].NodeUrl != leader {
		t.Errorf("For peer ID: %v, mismatch of URL for leader. Expected: '%v', Actual: '%v'", peer.id, leader, clusNodes[leaderNode])
	}
	var clusMembers []string
	for _, clusNode := range clusNodes {
		clusMembers = append(clusMembers, clusNode.NodeUrl)
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
		raft.NodeUrl(peerAddr),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.LeaseBasedReads(false),
		raft.SnapshotCatchUpEntries(25),
		raft.SnapshotCount(50),
		raft.MaxWALFiles(1),
		raft.MaxSnapFiles(1),
	)
	if err != nil {
		return nil, err
	} else {
		repl := NewReplicator(db, opts)
		return &peer{repl.node.id, db, repl}, nil
	}
}

func newPeer(id int) (*peer, error) {
	memKVStore := newInMemKVStore()
	return newPeerWithDB(id, memKVStore)
}

func newJoiningPeer(peerAddr string) (*peer, error) {
	opts, err := raft.NewOptions(
		raft.NodeUrl(peerAddr),
		raft.LogDir(logDir),
		raft.SnapDir(snapDir),
		raft.ClusterUrl(clusterUrl),
		raft.ReplicationTimeout(replTimeout),
		raft.LeaseBasedReads(false),
		raft.SnapshotCatchUpEntries(25),
		raft.SnapshotCount(50),
		raft.MaxWALFiles(1),
		raft.MaxSnapFiles(1),
	)
	if err != nil {
		return nil, err
	} else {
		memKVStore := newInMemKVStore()
		repl := NewReplicator(memKVStore, opts)
		return &peer{repl.node.id, memKVStore, repl}, nil
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
	mu        sync.Mutex
	content   map[string]interface{}
	currEntry db.RaftEntry
}

func newInMemKVStore() *inMemKVStore {
	return &inMemKVStore{content: make(map[string]interface{})}
}

func (this *inMemKVStore) Close() (_ error) {
	return
}

func (this *inMemKVStore) GetLastAppliedEntry() (db.RaftEntry, error) {
	return this.currEntry, nil
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

func (this *inMemKVStore) Save(raftEntry db.RaftEntry, data []byte) ([]byte, error) {
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
			this.currEntry = raftEntry
			return nil, nil
		}
	}
}

const (
	raftTermKey  = "raft.term"
	raftIndexKey = "raft.index"
)

func (this *inMemKVStore) Backup(_ db.SnapshotState) (io.ReadCloser, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.content[raftTermKey] = strconv.Itoa(int(this.currEntry.Term))
	this.content[raftIndexKey] = strconv.Itoa(int(this.currEntry.Index))
	defer func() {
		delete(this.content, raftTermKey)
		delete(this.content, raftIndexKey)
	}()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this.content); err != nil {
		return nil, err
	}
	return ioutil.NopCloser(&buf), nil
}

func (this *inMemKVStore) Restore(data io.ReadCloser) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	content := make(map[string]interface{})
	if err := gob.NewDecoder(data).Decode(&content); err != nil {
		return err
	} else {
		term, _ := strconv.Atoi(content[raftTermKey].(string))
		index, _ := strconv.Atoi(content[raftIndexKey].(string))
		this.currEntry = db.RaftEntry{Term: uint64(term), Index: uint64(index)}
		delete(content, raftTermKey)
		delete(content, raftIndexKey)
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

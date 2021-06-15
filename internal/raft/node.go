// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"golang.org/x/sync/semaphore"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/flipkart-incubator/nexus/internal/stats"
	pkg_raft "github.com/flipkart-incubator/nexus/pkg/raft"

	etcd_stats "github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

const (
	purgeFileInterval = 30 * time.Second
)

// A key-value stream backed by raft
type raftNode struct {
	readStateC chan raft.ReadState // to send out readState
	commitC    chan *raftpb.Entry  // entries committed to log (k,v)
	errorC     chan error          // errors from raft session

	id          uint64 // client ID for raft session
	cid         uint64 //clusterId
	join        bool   // node is joining an existing cluster
	waldir      string // path to WAL directory
	snapdir     string // path to snapshot directory
	getSnapshot func() ([]byte, error)
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	transport  *rafthttp.Transport
	stopc      chan struct{} // signals proposal channel closed
	httpstopc  chan struct{} // signals http server to shutdown
	httpdonec  chan struct{} // signals http server shutdown complete
	readOption raft.ReadOnlyOption
	statsCli   stats.Client
	rpeers     map[uint64]string

	snapSem                *semaphore.Weighted
	snapCount              uint64
	snapshotCatchUpEntries uint64
	maxSnapFiles           uint
	maxWALFiles            uint
}

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(opts pkg_raft.Options, statsCli stats.Client, getSnapshot func() ([]byte, error)) *raftNode {

	readStateC := make(chan raft.ReadState)
	commitC := make(chan *raftpb.Entry)
	errorC := make(chan error)
	nodeId := opts.NodeId()

	rc := &raftNode{
		readStateC:             readStateC,
		commitC:                commitC,
		errorC:                 errorC,
		id:                     nodeId,
		rpeers:                 opts.ClusterUrls(),
		join:                   opts.Join(),
		waldir:                 opts.LogDir(),
		snapdir:                opts.SnapDir(),
		getSnapshot:            getSnapshot,
		snapCount:              opts.SnapshotCount(),
		snapshotCatchUpEntries: opts.SnapshotCatchUpEntries(),
		stopc:                  make(chan struct{}),
		httpstopc:              make(chan struct{}),
		httpdonec:              make(chan struct{}),
		readOption:             opts.ReadOption(),
		statsCli:               statsCli,
		maxSnapFiles:           opts.MaxSnapFiles(),
		maxWALFiles:            opts.MaxWALFiles(),
		snapSem:                semaphore.NewWeighted(1),
		// rest of structure populated after WAL replay
	}

	rc.genClusterID()
	if rc.join {
		rc.rpeers[nodeId] = opts.NodeUrl().String()
	}
	return rc
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("[Node %x] first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", rc.id, firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *raftNode) getLeaderId() uint64 {
	return rc.node.Status().SoftState.Lead
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal: // nothing to do but leaving for clarity
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
					rc.rpeers[cc.NodeID] = string(cc.Context)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.id {
					log.Printf("[Node %x] I've been removed from the cluster! Shutting down.", rc.id)
					// TODO: In this case, check if its OK to not publish to rc.commitC
					return false
				}
				if _, ok := rc.rpeers[cc.NodeID]; !ok {
					log.Printf("[Node %x] WARNING Ignoring request to remove non-existing Node with ID: %v from the cluster.", rc.id, cc.NodeID)
				} else {
					rc.transport.RemovePeer(types.ID(cc.NodeID))
					delete(rc.rpeers, cc.NodeID)
				}
			}
		}

		select {
		case rc.commitC <- &ents[i]:
		case <-rc.stopc:
			return false
		}
		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("nexus.raft: [Node %x] error loading snapshot (%v)", rc.id, err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.MkdirAll(rc.waldir, 0750); err != nil {
			log.Fatalf("nexus.raft: [Node %x] cannot create dir for wal (%v)", rc.id, err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("nexus.raft: [Node %x] create wal error (%v)", rc.id, err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("[Node %x] loading WAL at term %d and index %d", rc.id, walsnap.Term, walsnap.Index)
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] error loading wal (%v)", rc.id, err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("[Node %x] replaying WAL", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] failed to read WAL (%v)", rc.id, err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) genClusterID() {
	//sort the id's first. This is required because clusterId should be constant
	// even if the member are written in any order.
	ids := make([]uint64, len(rc.rpeers))
	for id := range rc.rpeers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	b := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.BigEndian.PutUint64(b[8*i:], id)
	}
	hash := sha1.Sum(b)
	rc.cid = binary.BigEndian.Uint64(hash[:8])
	//log.Printf("genClusterID %+v Members %+v \n B Array %+v", rc.cid, mIDs, b)
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.MkdirAll(rc.snapdir, 0750); err != nil {
			log.Fatalf("nexus.raft: [Node %x] cannot create dir for snapshot (%v)", rc.id, err)
		}
	}
	rc.snapshotter = snap.New(rc.snapdir)

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	var rpeers []raft.Peer
	for id, peer := range rc.rpeers {
		rpeers = append(rpeers, raft.Peer{ID: id, Context: []byte(peer)})
	}
	c := &raft.Config{
		ID:              rc.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		ReadOnlyOption:  rc.readOption,
		CheckQuorum:     rc.readOption == raft.ReadOnlyLeaseBased,
	}

	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   types.ID(rc.cid),
		Raft:        rc,
		ServerStats: etcd_stats.NewServerStats("", ""),
		LeaderStats: etcd_stats.NewLeaderStats(strconv.Itoa(int(rc.id))),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i, peer := range rc.rpeers {
		if i != rc.id {
			rc.transport.AddPeer(types.ID(i), []string{peer})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("nexus.raft: [Node %x] publishing snapshot at index %d", rc.id, rc.snapshotIndex)
	defer log.Printf("nexus.raft: [Node %x] finished publishing snapshot at index %d", rc.id, rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("nexus.raft: [Node %x] snapshot index [%d] should > progress.appliedIndex [%d]", rc.id, snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}
	if !rc.snapSem.TryAcquire(1) {
		return //already running
	}
	defer rc.snapSem.Release(1)

	log.Printf("nexus.raft: [Node %x] start snapshot [applied index: %d | last snapshot index: %d]", rc.id, rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	// TODO: what should we do if the store fails to create snapshot() ?
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] get snapshot failed with error %v", rc.id, err)
	}
	//TODO: `rc.appliedIndex` might have changed by now. What should we do?
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		// the snapshot was done asynchronously with the progress of raft.
		// raft might have already got a newer snapshot.
		if err == raft.ErrSnapOutOfDate {
			return
		}
		log.Fatalf("nexus.raft: [Node %x] create snapshot failed with error %v", rc.id, err)
	}

	log.Printf("nexus.raft: [Node %x] created snapshot [applied index: %d | last snapshot index: %d]", rc.id, rc.appliedIndex, rc.snapshotIndex)
	if err := rc.saveSnap(snap); err != nil {
		log.Fatalf("nexus.raft: [Node %x] save snapshot failed with error %v", rc.id, err)
	}

	if rc.appliedIndex > rc.snapshotCatchUpEntries {
		compactIndex := rc.appliedIndex - rc.snapshotCatchUpEntries
		if err := rc.raftStorage.Compact(compactIndex); err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			log.Fatalf("nexus.raft: [Node %x] compaction failed with error %v", rc.id, err)
		}
		log.Printf("nexus.raft: [Node %x] compacted log at index %d", rc.id, compactIndex)
	}

	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) publishReadStates(readStates []raft.ReadState) bool {
	// TODO: We can just publish the latest read state like etcd
	for _, rs := range readStates {
		select {
		case rc.readStateC <- rs:
		case <-rc.stopc:
			return false
		}
	}

	return true
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case tick := <-ticker.C:
			rc.node.Tick()
			rc.statsCli.Timing("raft.tick.processing.latency.ms", tick)

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if ok := rc.publishReadStates(rd.ReadStates); !ok {
				rc.stop()
				return
			}
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			go rc.maybeTriggerSnapshot() //this can happen completely off the raft sm.
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.rpeers[rc.id])
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] Failed parsing URL (%v)", rc.id, err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] Failed to listen rafthttp (%v)", rc.id, err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("nexus.raft: [Node %x] Failed to serve rafthttp (%v)", rc.id, err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (rc *raftNode) purgeFile() {
	log.Printf("nexus.raft: [Node %x] Starting purgeFile() \n", rc.id)
	var serrc, werrc <-chan error
	if rc.maxSnapFiles > 0 {
		serrc = fileutil.PurgeFile(rc.snapdir, "snap", rc.maxSnapFiles, purgeFileInterval, rc.stopc)
	}
	if rc.maxWALFiles > 0 {
		werrc = fileutil.PurgeFile(rc.waldir, "wal", rc.maxWALFiles, purgeFileInterval, rc.stopc)
	}

	select {
	case e := <-serrc:
		log.Fatalf("nexus.raft: [Node %x] failed to purge snap file %s", rc.id, e.Error())
	case e := <-werrc:
		log.Fatalf("nexus.raft: [Node %x] failed to purge wal file %s", rc.id, e.Error())
	case <-rc.stopc:
		return
	}
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

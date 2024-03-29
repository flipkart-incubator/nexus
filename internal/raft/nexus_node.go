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
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/coreos/etcd/snap"
	"github.com/flipkart-incubator/nexus/internal/raft/storage"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"golang.org/x/sync/semaphore"

	"github.com/flipkart-incubator/nexus/internal/stats"
	pkg_raft "github.com/flipkart-incubator/nexus/pkg/raft"

	etcd_stats "github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

const (
	purgeFileInterval = 30 * time.Second
	// max number of in-flight snapshot messages nexus allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16
	sendSnapTimeout    = 10 * time.Second
)

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	data       []raftpb.Entry
	snapshot   raftpb.Snapshot
	applyDoneC chan struct{}
}

// A key-value stream backed by raft
type raftNode struct {
	readStateC chan raft.ReadState // to send out readState
	commitC    chan *apply         // entries committed to log (k,v)
	errorC     chan error          // errors from raft session
	msgSnapC   chan raftpb.Message // a chan to send/receive snapshot

	id          uint64 // client ID for raft session
	cid         uint64 //clusterId
	join        bool   // node is joining an existing cluster
	entDir      string // path for storing entries
	waldir      string // path to WAL directory
	snapdir     string // path to snapshot directory
	getSnapshot func(db.SnapshotState) (io.ReadCloser, error)
	lastIndex   uint64 // index of log at start
	entType     string //entryStore format  memory/disk

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage storage.RaftStorageEngine
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	transport  *rafthttp.Transport
	stopc      chan struct{} // signals proposal channel closed
	httpstopc  chan struct{} // signals http server to shutdown
	httpdonec  chan struct{} // signals http server shutdown complete
	readOption raft.ReadOnlyOption
	statsCli   stats.Client
	rpeers     map[uint64]string
	snapSem    *semaphore.Weighted

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
func NewRaftNode(opts pkg_raft.Options, statsCli stats.Client, store db.Store) *raftNode {

	readStateC := make(chan raft.ReadState)
	commitC := make(chan *apply)
	errorC := make(chan error)
	nodeId := opts.NodeId()

	rc := &raftNode{
		readStateC:             readStateC,
		commitC:                commitC,
		errorC:                 errorC,
		id:                     nodeId,
		rpeers:                 opts.ClusterUrls(),
		join:                   opts.Join(),
		entDir:                 opts.EntryDir(),
		entType:                opts.EntryStoreType(),
		waldir:                 opts.LogDir(),
		snapdir:                opts.SnapDir(),
		getSnapshot:            store.Backup,
		snapCount:              opts.SnapshotCount(),
		snapshotCatchUpEntries: opts.SnapshotCatchUpEntries(),
		stopc:                  make(chan struct{}),
		httpstopc:              make(chan struct{}),
		httpdonec:              make(chan struct{}),
		readOption:             opts.ReadOption(),
		statsCli:               statsCli,
		maxSnapFiles:           opts.MaxSnapFiles(),
		maxWALFiles:            opts.MaxWALFiles(),
		msgSnapC:               make(chan raftpb.Message, maxInFlightMsgSnap),
		snapSem:                semaphore.NewWeighted(1),

		// rest of structure populated after WAL replay
	}

	if lastAppliedEntry, err := store.GetLastAppliedEntry(); err == nil {
		rc.appliedIndex = lastAppliedEntry.Index
	}

	if rc.cid = opts.ClusterId(); rc.cid == 0 {
		rc.genClusterID()
	}

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
		//ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
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
func (rc *raftNode) publishEntries(ents []raftpb.Entry, snap raftpb.Snapshot) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

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
					return nil, false
				}
				if _, ok := rc.rpeers[cc.NodeID]; !ok {
					log.Printf("[Node %x] WARNING Ignoring request to remove non-existing Node with ID: %v from the cluster.", rc.id, cc.NodeID)
				} else {
					rc.transport.RemovePeer(types.ID(cc.NodeID))
					delete(rc.rpeers, cc.NodeID)
				}
			}
		}
	}

	applyDoneC := make(chan struct{}, 1)
	select {
	case rc.commitC <- &apply{data: ents, applyDoneC: applyDoneC, snapshot: snap}:
	case <-rc.stopc:
		return nil, false
	}

	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		snapshot, err := rc.snapshotter.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("nexus.raft: [Node %x] error loading snapshot (%v)", rc.id, err)
		}
		return snapshot
	}
	return nil
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
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
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
	var err error

	switch rc.entType {
	case "disk":
		if rc.raftStorage, err = storage.NewEntryStore(rc.entDir); err != nil {
			log.Fatalf("[Node %x] unable to create entry store, error: %v", rc.id, err)
		}
	default:
		rc.raftStorage = raft.NewMemoryStorage()
	}

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
		Applied:         rc.appliedIndex,
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
		Snapshotter: rc.snapshotter,
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

	if entStore, ok := rc.raftStorage.(*storage.EntryStore); ok {
		entStore.Close()
	}

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

	log.Printf("nexus.raft: [Node %x] applying snapshot at index %d", rc.id, rc.appliedIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("nexus.raft: [Node %x] snapshot index [%d] should > progress.appliedIndex [%d]", rc.id, snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index

	log.Printf("nexus.raft: [Node %x] finished applying snapshot new index %d", rc.id, rc.appliedIndex)
}

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	//TODO: create a quick copy of the kvstore and pass it to goroutine.

	//async snapshot. Release raft routine
	go func() {

		// wait until all committed entries are applied (or server is closed)
		if applyDoneC != nil {
			select {
			case <-applyDoneC:
			case <-rc.stopc:
				return
			}
		}

		if !rc.snapSem.TryAcquire(1) {
			return //already running
		}
		rc.triggerSnapshot()
		rc.snapSem.Release(1)

	}()
}

func (rc *raftNode) triggerSnapshot() {

	//re-check since a previous snapshot may just have got completd and the semaphore was just released.
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		log.Printf("nexus.raft: [Node %x] skipped snapshot [applied index: %d | last snapshot index: %d]", rc.id, rc.appliedIndex, rc.snapshotIndex)
		return
	}

	appliedIndex := rc.appliedIndex
	log.Printf("nexus.raft: [Node %x] start snapshot async [applied index: %d | last snapshot index: %d]", rc.id, appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot(db.SnapshotState{SnapshotIndex: rc.snapshotIndex, AppliedIndex: appliedIndex})
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] get snapshot failed with error %v", rc.id, err)
	}
	defer data.Close()

	snapshot, err := rc.raftStorage.CreateSnapshot(appliedIndex, &rc.confState, nil)
	if err != nil {
		// the snapshot was done asynchronously with the progress of raft.
		// raft might have already got a newer snapshot.
		if err == raft.ErrSnapOutOfDate {
			return
		}
		log.Fatalf("nexus.raft: [Node %x] create snapshot failed with error %v", rc.id, err)
	}
	if _, err = rc.snapshotter.SaveDBFrom(data, appliedIndex); err != nil {
		log.Fatal(err)
	}

	log.Printf("nexus.raft: [Node %x] created snapshot [applied index: %d | last snapshot index: %d]", rc.id, appliedIndex, rc.snapshotIndex)
	if err = rc.saveSnap(snapshot); err != nil {
		log.Fatalf("nexus.raft: [Node %x] save snapshot failed with error %v", rc.id, err)
	}

	if appliedIndex > rc.snapshotCatchUpEntries {
		compactIndex := appliedIndex - rc.snapshotCatchUpEntries
		if err = rc.raftStorage.Compact(compactIndex); err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			log.Fatalf("nexus.raft: [Node %x] compaction failed with error %v", rc.id, err)
		}
		log.Printf("nexus.raft: [Node %x] compacted log at index %d", rc.id, compactIndex)
	}

	rc.snapshotIndex = appliedIndex
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
	// Set appliedIndex only if its not already initialised
	// Note that we also set appliedIndex during init from
	// storage supplied value.
	if rc.appliedIndex == 0 {
		rc.appliedIndex = snap.Metadata.Index
	}

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

			if err = rc.wal.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatalf("nexus.raft: [Node %x] raft save state and entries error: %v", rc.id, err)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			processedMsgs := rc.processMessages(rd.Messages)
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries), rd.Snapshot)
			if !ok {
				rc.stop()
				return
			}
			//TODO: this should be moved to the applyChannel consumer.
			// Raft thread should not be blocked for snapshots which can take long time to complete.
			rc.maybeTriggerSnapshot(applyDoneC)
			rc.transport.Send(processedMsgs)

			rc.node.Advance()

		case err = <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := len(ms) - 1; i >= 0; i-- {
		if r.IsIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgSnap {
			// The msgSnap only contains the most recent id of store without KV.
			// So we need to redirect the msgSnap to replicator loop for sending merged snap
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0 //don't forward this msg.
		}
	}
	return ms
}

func (rc *raftNode) serveRaft() {
	_url, err := url.Parse(rc.rpeers[rc.id])
	if err != nil {
		log.Fatalf("nexus.raft: [Node %x] Failed parsing URL (%v)", rc.id, err)
	}

	ln, err := newStoppableListener(_url.Host, rc.httpstopc)
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
func (rc *raftNode) IsIDRemoved(id uint64) bool { return false }

func (rc *raftNode) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}

func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

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
	var serrc, werrc, derrc <-chan error
	if rc.maxSnapFiles > 0 {
		serrc = fileutil.PurgeFile(rc.snapdir, "snap", rc.maxSnapFiles, purgeFileInterval, rc.stopc)
		derrc = fileutil.PurgeFile(rc.snapdir, "snap.db", rc.maxSnapFiles, purgeFileInterval, rc.stopc)
	}

	if rc.maxWALFiles > 0 {
		werrc = fileutil.PurgeFile(rc.waldir, "wal", rc.maxWALFiles, purgeFileInterval, rc.stopc)
	}

	select {
	case e := <-serrc:
		log.Fatalf("nexus.raft: [Node %x] failed to purge snap file %s", rc.id, e.Error())
	case e := <-derrc:
		log.Fatalf("nexus.raft: [Node %x] failed to purge snap.db file %s", rc.id, e.Error())
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

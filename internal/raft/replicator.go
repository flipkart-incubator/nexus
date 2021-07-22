package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/flipkart-incubator/nexus/internal/models"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/flipkart-incubator/nexus/internal/stats"
	"github.com/flipkart-incubator/nexus/pkg/db"
	pkg_raft "github.com/flipkart-incubator/nexus/pkg/raft"
)

type internalNexusResponse struct {
	Res []byte
	Err error
}

type replicator struct {
	node            *raftNode
	store           db.Store
	confChangeCount uint64
	waiter          wait.Wait
	applyWait       wait.WaitTime
	idGen           *idutil.Generator
	statsCli        stats.Client
	opts            pkg_raft.Options
}

const (
	MetricPrefix     = "nexus."
	NodeIdDefaultTag = "nexusNode"
)

func initStatsD(opts pkg_raft.Options) stats.Client {
	if statsdAddr := opts.StatsDAddr(); statsdAddr != "" {
		return stats.NewStatsDClient(statsdAddr, MetricPrefix,
			stats.NewTag(NodeIdDefaultTag, opts.NodeUrl().Host))
	}
	return stats.NewNoOpClient()
}

func NewReplicator(store db.Store, options pkg_raft.Options) *replicator {
	statsCli := initStatsD(options)
	raftNode := NewRaftNode(options, statsCli, store)
	repl := &replicator{
		node:            raftNode,
		store:           store,
		confChangeCount: uint64(0),
		waiter:          wait.New(),
		applyWait:       wait.NewTimeList(),
		idGen:           idutil.NewGenerator(uint16(raftNode.id), time.Now()),
		statsCli:        statsCli,
		opts:            options,
	}
	return repl
}

func (this *replicator) Id() uint64 {
	return this.node.id
}

func (this *replicator) Start() {
	this.node.startRaft()
	this.restoreFromSnapshot()
	//go this.readCommits()
	go this.applyAll()
	go this.readReadStates()
	go this.node.purgeFile()
}

func (this *replicator) ListMembers() (uint64, map[uint64]string) {
	lead := this.node.getLeaderId()
	peers := this.node.rpeers
	return lead, peers
}

func (this *replicator) Save(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	defer this.statsCli.Timing("save.latency.ms", time.Now())
	repl_req := &models.NexusInternalRequest{ID: this.idGen.Next(), Req: data}
	if repl_req_data, err := proto.Marshal(repl_req); err != nil {
		this.statsCli.Incr("save.marshal.error", 1)
		return nil, err
	} else {
		ch := this.waiter.Register(repl_req.ID)
		child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
		defer cancel()
		if err = this.node.node.Propose(child_ctx, repl_req_data); err != nil {
			log.Printf("[WARN] [Node %x] Error while proposing to Raft. Message: %v.", this.node.id, err)
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			this.statsCli.Incr("raft.propose.error", 1)
			return nil, err
		}
		select {
		case res := <-ch:
			repl_res := res.(*internalNexusResponse)
			return repl_res.Res, repl_res.Err
		case <-child_ctx.Done():
			err := child_ctx.Err()
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			this.statsCli.Incr("save.timeout.error", 1)
			return nil, err
		}
	}
}

func (this *replicator) Load(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	defer this.statsCli.Timing("load.latency.ms", time.Now())
	readReqId := this.idGen.Next()
	ch := this.waiter.Register(readReqId)
	child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
	defer cancel()
	idData := make([]byte, 8)
	binary.BigEndian.PutUint64(idData, readReqId)
	if err := this.node.node.ReadIndex(child_ctx, idData); err != nil {
		log.Printf("[WARN] [Node %x] Error while reading index in Raft. Message: %v.", this.node.id, err)
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
		return nil, err
	}
	select {
	case res := <-ch:
		if inr := res.(*internalNexusResponse); inr.Err != nil {
			this.statsCli.Incr("raft.read.index.error", 1)
			return nil, inr.Err
		} else {
			index := binary.BigEndian.Uint64(inr.Res)
			select {
			case <-this.applyWait.Wait(index):
			case <-child_ctx.Done():
				return nil, child_ctx.Err()
			}
			res, err := this.store.Load(data)
			return res, err
		}
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
		this.statsCli.Incr("load.timeout.error", 1)
		return nil, err
	}
}

func (this *replicator) AddMember(ctx context.Context, nodeUrl string) error {
	nodeOpts, err := pkg_raft.NewOptions(pkg_raft.NodeUrl(nodeUrl))
	if err != nil {
		return err
	}
	nodeAddr := nodeOpts.NodeUrl()
	if _, err = net.Dial("tcp", nodeAddr.Host); err != nil {
		return fmt.Errorf("unable to verify RAFT service running at %s, error: %v", nodeAddr, err)
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeOpts.NodeId(),
		Context: []byte(nodeAddr.String()),
	}
	return this.proposeConfigChange(ctx, cc)
}

func (this *replicator) RemoveMember(ctx context.Context, nodeUrl string) error {
	nodeOpts, err := pkg_raft.NewOptions(pkg_raft.NodeUrl(nodeUrl))
	if err != nil {
		return err
	}
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: nodeOpts.NodeId()}
	return this.proposeConfigChange(ctx, cc)
}

func (this *replicator) Stop() {
	close(this.node.stopc)
	this.store.Close()
	this.statsCli.Close()
}

func (this *replicator) proposeConfigChange(ctx context.Context, confChange raftpb.ConfChange) error {
	defer this.statsCli.Timing("config.change.latency.ms", time.Now())
	confChange.ID = atomic.AddUint64(&this.confChangeCount, 1)
	ch := this.waiter.Register(confChange.ID)
	child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
	defer cancel()
	if err := this.node.node.ProposeConfChange(ctx, confChange); err != nil {
		log.Printf("[WARN] [Node %x] Error while proposing config change to Raft. Message: %v.", this.node.id, err)
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		return err
	}
	select {
	case res := <-ch:
		if err := res.(*internalNexusResponse).Err; err != nil {
			this.statsCli.Incr("config.change.error", 1)
			return err
		}
		return nil
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		this.statsCli.Incr("config.change.timeout.error", 1)
		return err
	}
}

func (this *replicator) restoreFromSnapshot() {
	snapshot, err := this.node.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		log.Printf("[Node %x] WARNING - Received no snapshot error", this.node.id)
		return
	}
	if err != nil {
		log.Panic(err)
	}
	log.Printf("[Node %x] Loading snapshot at term %d and index %d", this.node.id, snapshot.Metadata.Term, snapshot.Metadata.Index)

	//readId file from snapPayload
	//indexId := binary.LittleEndian.Uint64(snapshot.Data)
	dbFile, err := this.node.snapshotter.DBFilePath(snapshot.Metadata.Index)
	if err != nil {
		log.Printf("[Node %x] Failed to load db file for snapshot index %d", this.node.id, snapshot.Metadata.Index)
		return
	}

	reader, err := os.Open(dbFile)
	if err != nil {
		log.Panic(err)
	}

	if err = this.store.Restore(reader); err != nil {
		log.Panic(err)
	}
	log.Printf("[Node %x] Restored snapshot at term %d and index %d", this.node.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
}


func (r *replicator) applyAll() {
	for ap := range r.node.applyc {
		r.applySnapshot(ap)
		r.applyEntries(ap)
		//s.applyWait.Trigger(ep.appliedi)
		//r.applyWait.Trigger(r.node.appliedIndex)

		//// wait for the raft routine to finish the disk writes before triggering a
		//// snapshot. or applied index might be greater than the last index in raft
		//// storage, since the raft routine might be slower than apply routine.
		<-ap.notifyc

		r.sendSnapshots()
	}
	//s.applySnapshot(ep, apply)
	//s.applyEntries(ep, apply)
	//
	//proposalsApplied.Set(float64(ep.appliedi))
	//s.applyWait.Trigger(ep.appliedi)
	//// wait for the raft routine to finish the disk writes before triggering a
	//// snapshot. or applied index might be greater than the last index in raft
	//// storage, since the raft routine might be slower than apply routine.
	//<-apply.notifyc
	//
	//s.triggerSnapshot(ep)
	//select {
	//// snapshot requested via send()
	//case m := <-s.r.msgSnapC:
	//	merged := s.createMergedSnapshotMessage(m, ep.appliedt, ep.appliedi, ep.confState)
	//	s.sendMergedSnap(merged)
	//default:
	//}
}

func (rc *replicator) applyEntries(apply *apply)  {
	if len(apply.data) == 0 {
		return
	}
	firstIdx := apply.data[0].Index
	if firstIdx > rc.node.appliedIndex+1 {
		log.Fatalf("[Node %x] first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", rc.node.id, firstIdx, rc.node.appliedIndex)
	}
	var ents []raftpb.Entry
	if rc.node.appliedIndex-firstIdx+1 < uint64(len(apply.data)) {
		ents = apply.data[rc.node.appliedIndex-firstIdx+1:]
	}
	if len(ents) == 0 {
		return
	}

	rc.apply(ents)
}

func (rc *replicator) apply(es []raftpb.Entry) (shouldStop bool) {
	for i := range es {
		e := es[i]
		switch e.Type {
		case raftpb.EntryNormal:
			rc.applyEntryNormal(&e)
			rc.node.appliedIndex = e.Index
		case raftpb.EntryConfChange:
			//// set the consistent index of current executing entry
			//if e.Index > s.consistIndex.ConsistentIndex() {
			//	s.consistIndex.setConsistentIndex(e.Index)
			//}

			_ = rc.applyConfChange(&e)
			rc.node.appliedIndex = e.Index
			//shouldStop = shouldStop || removedSelf
			//s.w.Trigger(cc.ID, &confChangeResponse{s.cluster.Members(), err})
		default:
			log.Panicf("entry type should be either EntryNormal or EntryConfChange")
		}
		rc.applyWait.Trigger(e.Index)

	}

	return false
}

func (rc *replicator) applyEntryNormal(e *raftpb.Entry) {
	var replReq models.NexusInternalRequest
	if err := proto.Unmarshal(e.Data, &replReq); err != nil {
		log.Fatal(err)
	} else {
		replRes := internalNexusResponse{}
		raftEntry := db.RaftEntry{Index: e.Index, Term: e.Term}
		replRes.Res, replRes.Err = rc.store.Save(raftEntry, replReq.Req)
		rc.waiter.Trigger(replReq.ID, &replRes)
	}
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *replicator) applyConfChange(e *raftpb.Entry)   error {
	var cc raftpb.ConfChange
	cc.Unmarshal(e.Data)
	rc.node.confState = *rc.node.node.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			rc.node.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
			rc.node.rpeers[cc.NodeID] = string(cc.Context)
		}
	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == rc.node.id {
			log.Printf("[Node %x] I've been removed from the cluster! Shutting down.", rc.node.id)
			// TODO: In this case, check if its OK to not publish to rc.commitC
			return nil
		}
		if _, ok := rc.node.rpeers[cc.NodeID]; !ok {
			log.Printf("[Node %x] WARNING Ignoring request to remove non-existing Node with ID: %v from the cluster.", rc.node.id, cc.NodeID)
		} else {
			rc.node.transport.RemovePeer(types.ID(cc.NodeID))
			delete(rc.node.rpeers, cc.NodeID)
		}
	}
	rc.waiter.Trigger(cc.ID, &internalNexusResponse{e.Data, nil})

	return nil
}

func (r *replicator) applySnapshot(apply *apply) {
	if raft.IsEmptySnap(apply.snapshot) {
		return
	}

	log.Printf("nexus.raft: [Node %x] publishing snapshot at index %d", r.node.id, r.node.snapshotIndex)
	defer log.Printf("nexus.raft: [Node %x] finished publishing snapshot at index %d", r.node.id, r.node.snapshotIndex)

	if apply.snapshot.Metadata.Index <= r.node.appliedIndex {
		log.Fatalf("nexus.raft: [Node %x] snapshot index [%d] should > progress.appliedIndex [%d]", r.node.id, apply.snapshot.Metadata.Index, r.node.appliedIndex)
	}

	// wait for raftNode to persist snapshot onto the disk
	fmt.Println("Waiting on apply.notifyc")
	<-apply.notifyc
	fmt.Println("Finished on apply.notifyc")

	r.restoreFromSnapshot()
	//rc.commitC <- nil // trigger kvstore to load snapshot

	r.node.confState = apply.snapshot.Metadata.ConfState
	r.node.snapshotIndex = apply.snapshot.Metadata.Index
	r.node.appliedIndex = apply.snapshot.Metadata.Index
}

func (this *replicator) readCommits() {
//	for entry := range this.node.commitC {
//		if entry == nil {
//			log.Printf("[Node %x] Received a message in the commit channel with no data", this.node.id)
//			this.restoreFromSnapshot()
//			continue
//		}
//
//		if len(entry.Data) > 0 {
//			switch entry.Type {
//			case raftpb.EntryNormal:
//				var replReq models.NexusInternalRequest
//				if err := proto.Unmarshal(entry.Data, &replReq); err != nil {
//					log.Fatal(err)
//				} else {
//					replRes := internalNexusResponse{}
//					raftEntry := db.RaftEntry{Index: entry.Index, Term: entry.Term}
//					replRes.Res, replRes.Err = this.store.Save(raftEntry, replReq.Req)
//					this.waiter.Trigger(replReq.ID, &replRes)
//				}
//			case raftpb.EntryConfChange:
//				var cc raftpb.ConfChange
//				if err := cc.Unmarshal(entry.Data); err != nil {
//					log.Fatal(err)
//				} else {
//					this.waiter.Trigger(cc.ID, &internalNexusResponse{entry.Data, nil})
//				}
//			}
//		}
//		// signal any linearizable reads blocked for this index
//		this.applyWait.Trigger(entry.Index)
//
//
//		//if err := this.sendSnapshots(); err != nil {
//		//	log.Fatal(err)
//		//}
//	}
//
//	if err, present := <-this.node.errorC; present {
//		log.Fatal(err)
//	}
}

func (this *replicator) sendSnapshots() error {
	select {
	// snapshot requested via send()
	case m := <-this.node.msgSnapC:
		log.Printf("nexus.raft: [Node %x] Request to send snapshot to  %d at Term %d, Index %d \n", this.node.id, m.To, m.Term, m.Index)

		//load latest snapshot
		currentSnap, err := this.node.snapshotter.Load()
		if err != nil {
			return err
		}

		// put the []byte snapshot of store into raft snapshot and return the merged snapshot with
		// KV readCloser snapshot.
		snapshot := raftpb.Snapshot{
			Metadata: currentSnap.Metadata,
			Data:     nil,
		}
		m.Snapshot = snapshot

		//file data
		//indexId := binary.LittleEndian.Uint64(currentSnap.Data)
		dbFile, err := this.node.snapshotter.DBFilePath(currentSnap.Metadata.Index)
		if err != nil {
			return err
		}
		rc, err := os.Open(dbFile)
		if err != nil {
			return err
		}
		stat, _ := rc.Stat()
		mergedSnap := *snap.NewMessage(m, rc, stat.Size())

		log.Printf("nexus.raft: [Node %x] Sending snap(T%d)+db(%s) snapshot to  %x \n", this.node.id, snapshot.Metadata.Index ,path.Base(dbFile), m.To)

		//atomic.AddInt64(&s.inflightSnapshots, 1)
		this.node.transport.SendSnapshot(mergedSnap)
		//go func() {
		//	select {
		//	case ok := <-merged.CloseNotify():
		//		// delay releasing inflight snapshot for another 30 seconds to
		//		// block log compaction.
		//		// If the follower still fails to catch up, it is probably just too slow
		//		// to catch up. We cannot avoid the snapshot cycle anyway.
		//		if ok {
		//			select {
		//			case <-time.After(releaseDelayAfterSnapshot):
		//			case <-s.stopping:
		//			}
		//		}
		//		atomic.AddInt64(&s.inflightSnapshots, -1)
		//	case <-s.stopping:
		//		return
		//	}
		//}()
		default:
		// No pending snapshot request
	}

	return nil
}

func (this *replicator) readReadStates() {
	for rd := range this.node.readStateC {
		id := binary.BigEndian.Uint64(rd.RequestCtx)
		indexData := make([]byte, 8)
		binary.BigEndian.PutUint64(indexData, rd.Index)
		this.waiter.Trigger(id, &internalNexusResponse{indexData, nil})
	}
}

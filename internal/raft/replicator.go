package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/flipkart-incubator/nexus/pkg/db"
	pkg_raft "github.com/flipkart-incubator/nexus/pkg/raft"
	"github.com/smira/go-statsd"
)

type internalNexusRequest struct {
	ID  uint64
	Req []byte
}

type internalNexusResponse struct {
	Res []byte
	Err error
}

func (this *internalNexusRequest) marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (this *internalNexusRequest) unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(this)
}

type replicator struct {
	node            *raftNode
	store           db.Store
	confChangeCount uint64
	waiter          wait.Wait
	applyWait       wait.WaitTime
	idGen           *idutil.Generator
	stats           *statsd.Client
	opts            pkg_raft.Options
}

const (
	MetricPrefix     = "nexus."
	NodeIdDefaultTag = "nodeId"
)

func initStatsD(opts pkg_raft.Options) *statsd.Client {
	if statsdAddr := opts.StatsDAddr(); statsdAddr != "" {
		return statsd.NewClient(statsdAddr,
			statsd.TagStyle(statsd.TagFormatDatadog),
			statsd.MetricPrefix(MetricPrefix),
			statsd.DefaultTags(statsd.StringTag(NodeIdDefaultTag, strconv.Itoa(opts.NodeId()))),
		)
	}
	return nil
}

func (this *replicator) incrStat(stat string, count int64, tags ...statsd.Tag) {
	if this.stats != nil {
		this.stats.Incr(stat, count, tags...)
	}
}

func (this *replicator) startTimeStat() int64 {
	if this.stats != nil {
		return time.Now().UnixNano() / 1e6
	}
	return 0
}

func (this *replicator) endTimeStat(stat string, startTime int64, tags ...statsd.Tag) {
	if this.stats != nil {
		endTime := time.Now().UnixNano() / 1e6
		this.stats.Timing(stat, endTime-startTime, tags...)
	}
}

func (this *replicator) gaugeStat(stat string, value int64, tags ...statsd.Tag) {
	if this.stats != nil {
		this.stats.Gauge(stat, value, tags...)
	}
}

func (this *replicator) gaugeDeltaStat(stat string, delta int64, tags ...statsd.Tag) {
	if this.stats != nil {
		this.stats.GaugeDelta(stat, delta, tags...)
	}
}

func (this *replicator) closeStats() {
	if this.stats != nil {
		this.stats.Close()
	}
}

func NewReplicator(store db.Store, options pkg_raft.Options) *replicator {
	raftNode := NewRaftNode(options, store.Backup)
	repl := &replicator{
		node:            raftNode,
		store:           store,
		confChangeCount: uint64(0),
		waiter:          wait.New(),
		applyWait:       wait.NewTimeList(),
		idGen:           idutil.NewGenerator(uint16(options.NodeId()), time.Now()),
		stats:           initStatsD(options),
		opts:            options,
	}
	return repl
}

func (this *replicator) Start() {
	go this.readCommits()
	go this.readReadStates()
	this.node.startRaft()
}

func (this *replicator) Save(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	defer this.endTimeStat("save.latency.ms", this.startTimeStat())
	repl_req := &internalNexusRequest{ID: this.idGen.Next(), Req: data}
	if repl_req_data, err := repl_req.marshal(); err != nil {
		this.incrStat("save.marshal.error", 1)
		return nil, err
	} else {
		ch := this.waiter.Register(repl_req.ID)
		child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
		defer cancel()
		if err := this.node.node.Propose(child_ctx, repl_req_data); err != nil {
			log.Printf("[WARN] [Node %v] Error while proposing to Raft. Message: %v.", this.node.id, err)
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			this.incrStat("raft.propose.error", 1)
			return nil, err
		}
		select {
		case res := <-ch:
			repl_res := res.(*internalNexusResponse)
			return repl_res.Res, repl_res.Err
		case <-child_ctx.Done():
			err := child_ctx.Err()
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			this.incrStat("save.timeout.error", 1)
			return nil, err
		}
	}
}

func (this *replicator) Load(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	defer this.endTimeStat("load.latency.ms", this.startTimeStat())
	readReqId := this.idGen.Next()
	ch := this.waiter.Register(readReqId)
	child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
	defer cancel()
	idData := make([]byte, 8)
	binary.BigEndian.PutUint64(idData, readReqId)
	if err := this.node.node.ReadIndex(child_ctx, idData); err != nil {
		log.Printf("[WARN] [Node %v] Error while reading index in Raft. Message: %v.", this.node.id, err)
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
		return nil, err
	}
	select {
	case res := <-ch:
		if inr := res.(*internalNexusResponse); inr.Err != nil {
			this.incrStat("raft.read.index.error", 1)
			return nil, inr.Err
		} else {
			index := binary.BigEndian.Uint64(inr.Res)
			if ai := this.node.appliedIndex; ai < index {
				log.Printf("[WARN] [Node %v] Waiting for read index to be applied. ReadIndex: %d, AppliedIndex: %d", this.node.id, index, ai)
				// wait for applied index to catchup
				select {
				case <-this.applyWait.Wait(index):
				case <-child_ctx.Done():
					return nil, child_ctx.Err()
				}
			}
			res, err := this.store.Load(data)
			return res, err
		}
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
		this.incrStat("load.timeout.error", 1)
		return nil, err
	}
}

func (this *replicator) AddMember(ctx context.Context, nodeId int, nodeUrl string) error {
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: uint64(nodeId), Context: []byte(nodeUrl)}
	return this.proposeConfigChange(ctx, cc)
}

func (this *replicator) RemoveMember(ctx context.Context, nodeId int) error {
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: uint64(nodeId)}
	return this.proposeConfigChange(ctx, cc)
}

func (this *replicator) Stop() {
	close(this.node.stopc)
	this.store.Close()
	this.closeStats()
}

func (this *replicator) proposeConfigChange(ctx context.Context, confChange raftpb.ConfChange) error {
	defer this.endTimeStat("config.change.latency.ms", this.startTimeStat())
	confChange.ID = atomic.AddUint64(&this.confChangeCount, 1)
	ch := this.waiter.Register(confChange.ID)
	child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
	defer cancel()
	if err := this.node.node.ProposeConfChange(ctx, confChange); err != nil {
		log.Printf("[WARN] [Node %v] Error while proposing config change to Raft. Message: %v.", this.node.id, err)
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		return err
	}
	select {
	case res := <-ch:
		if err := res.(*internalNexusResponse).Err; err != nil {
			this.incrStat("config.change.error", 1)
			return err
		}
		return nil
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		this.incrStat("config.change.timeout.error", 1)
		return err
	}
}

func (this *replicator) readCommits() {
	for entry := range this.node.commitC {
		if entry == nil {
			log.Printf("[Node %v] Received a message in the commit channel with no data", this.node.id)
			snapshot, err := this.node.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				log.Printf("[Node %v] WARNING - Received no snapshot error", this.node.id)
				continue
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("[Node %v] Loading snapshot at term %d and index %d", this.node.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := this.store.Restore(snapshot.Data); err != nil {
				log.Panic(err)
			}
		} else {
			if len(entry.Data) > 0 {
				switch entry.Type {
				case raftpb.EntryNormal:
					var repl_req internalNexusRequest
					if err := repl_req.unmarshal(entry.Data); err != nil {
						log.Fatal(err)
					} else {
						repl_res := internalNexusResponse{}
						repl_res.Res, repl_res.Err = this.store.Save(repl_req.Req)
						this.waiter.Trigger(repl_req.ID, &repl_res)
					}
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						log.Fatal(err)
					} else {
						this.waiter.Trigger(cc.ID, &internalNexusResponse{entry.Data, nil})
					}
				}
			}
			// signal any linearizable reads blocked for this index
			this.applyWait.Trigger(entry.Index)
		}
	}
	if err, present := <-this.node.errorC; present {
		log.Fatal(err)
	}
}

func (this *replicator) readReadStates() {
	for rd := range this.node.readStateC {
		id := binary.BigEndian.Uint64(rd.RequestCtx)
		indexData := make([]byte, 8)
		binary.BigEndian.PutUint64(indexData, rd.Index)
		this.waiter.Trigger(id, &internalNexusResponse{indexData, nil})
	}
}

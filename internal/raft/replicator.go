package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"net"
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
	raftNode := NewRaftNode(options, statsCli, store.Backup)
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
	go this.readCommits()
	go this.readReadStates()
	this.node.startRaft()
}

func (this *replicator) ListMembers() (uint64, map[uint64]string) {
	lead := this.node.getLeaderId()
	peers := this.node.rpeers
	return lead, peers
}

func (this *replicator) Save(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	defer this.statsCli.Timing("save.latency.ms", time.Now())
	repl_req := &internalNexusRequest{ID: this.idGen.Next(), Req: data}
	if repl_req_data, err := repl_req.marshal(); err != nil {
		this.statsCli.Incr("save.marshal.error", 1)
		return nil, err
	} else {
		ch := this.waiter.Register(repl_req.ID)
		child_ctx, cancel := context.WithTimeout(ctx, this.opts.ReplTimeout())
		defer cancel()
		if err := this.node.node.Propose(child_ctx, repl_req_data); err != nil {
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
	if _, err := net.Dial("tcp", nodeAddr.Host); err != nil {
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

func (this *replicator) readCommits() {
	for entry := range this.node.commitC {
		if entry == nil {
			log.Printf("[Node %x] Received a message in the commit channel with no data", this.node.id)
			snapshot, err := this.node.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				log.Printf("[Node %x] WARNING - Received no snapshot error", this.node.id)
				continue
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("[Node %x] Loading snapshot at term %d and index %d", this.node.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
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

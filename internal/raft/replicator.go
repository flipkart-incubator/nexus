package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/flipkart-incubator/nexus/pkg/db"
	pkg_raft "github.com/flipkart-incubator/nexus/pkg/raft"
)

type replicator struct {
	node            *raftNode
	store           db.Store
	confChangeCount uint64
	replTimeout     time.Duration
	waiter          wait.Wait
	idGen           *idutil.Generator
}

type internalNexusRequest struct {
	ID      uint64
	Req     []byte
	ConfReq []byte
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

func (this *replicator) Start() {
	go this.node.startRaft()
	<-this.node.snapshotterReady
	go this.readCommits()
	go this.readReadStates()
}

func (this *replicator) Save(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	repl_req := &internalNexusRequest{ID: this.idGen.Next(), Req: data}
	if repl_req_data, err := repl_req.marshal(); err != nil {
		return nil, err
	} else {
		ch := this.waiter.Register(repl_req.ID)
		child_ctx, cancel := context.WithTimeout(ctx, this.replTimeout)
		defer cancel()
		if err := this.node.node.Propose(child_ctx, repl_req_data); err != nil {
			log.Printf("[WARN] Error while proposing to Raft. Message: %v.", err)
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			return nil, err
		}
		select {
		case res := <-ch:
			repl_res := res.(*internalNexusResponse)
			return repl_res.Res, repl_res.Err
		case <-child_ctx.Done():
			err := child_ctx.Err()
			this.waiter.Trigger(repl_req.ID, &internalNexusResponse{Err: err})
			return nil, err
		}
	}
}

func (this *replicator) Load(ctx context.Context, data []byte) ([]byte, error) {
	// TODO: Validate raft state to check if Start() has been invoked
	readReqId := this.idGen.Next()
	ch := this.waiter.Register(readReqId)
	child_ctx, cancel := context.WithTimeout(ctx, this.replTimeout)
	defer cancel()
	idData := make([]byte, 8)
	binary.BigEndian.PutUint64(idData, readReqId)
	if err := this.node.node.ReadIndex(child_ctx, idData); err != nil {
		log.Printf("[WARN] Error while reading index in Raft. Message: %v.", err)
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
		return nil, err
	}
	select {
	case indexData := <-ch:
		index := binary.BigEndian.Uint64(indexData.([]byte))
		if ai := this.node.appliedIndex; ai >= index {
			return this.store.Load(data)
		} else {
			// TODO: Implement this case by waiting for apply
			log.Printf("[WARN} Unable to read. Haven't applied this index yet. ReadIndex: %d, AppliedIndex: %d", index, ai)
			return nil, errors.New("Not yet applied the read index")
		}
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(readReqId, &internalNexusResponse{Err: err})
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
}

func NewReplicator(store db.Store, options pkg_raft.Options) *replicator {
	raftNode := NewRaftNode(options, store.Backup)
	repl := &replicator{
		node:            raftNode,
		store:           store,
		confChangeCount: uint64(0),
		replTimeout:     options.ReplTimeout(),
		waiter:          wait.New(),
		idGen:           idutil.NewGenerator(uint16(options.NodeId()), time.Now()),
	}
	return repl
}

func (this *replicator) proposeConfigChange(ctx context.Context, confChange raftpb.ConfChange) error {
	confChange.ID = atomic.AddUint64(&this.confChangeCount, 1)
	ch := this.waiter.Register(confChange.ID)
	child_ctx, cancel := context.WithTimeout(ctx, this.replTimeout)
	defer cancel()
	if err := this.node.node.ProposeConfChange(ctx, confChange); err != nil {
		log.Printf("[WARN] Error while proposing config change to Raft. Message: %v.", err)
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		return err
	}
	select {
	case <-ch:
		return nil
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		return err
	}
}

func (this *replicator) readCommits() {
	for entry := range this.node.commitC {
		if entry == nil {
			log.Printf("[%d] Received a message in the commit channel with no data", this.node.id)
			snapshot, err := this.node.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				log.Printf("[%d] WARNING - Received no snapshot error", this.node.id)
				continue
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("[%d] Loading snapshot at term %d and index %d", this.node.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
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
						this.waiter.Trigger(cc.ID, cc)
					}
				}
			}
			// after commit, update appliedIndex
			this.node.appliedIndex = entry.Index
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
		this.waiter.Trigger(id, indexData)
	}
}

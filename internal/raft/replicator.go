package raft

import (
	"bytes"
	"context"
	"encoding/gob"
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
	node                 *raftNode
	store                db.Store
	confChangeCount      uint64
	commitChan           <-chan []byte
	errorChan            <-chan error
	snapshotterReadyChan <-chan struct{}
	replTimeout          time.Duration
	waiter               wait.Wait
	idGen                *idutil.Generator
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

func unmarshal(data []byte) (*internalNexusRequest, error) {
	save_req := internalNexusRequest{}
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&save_req); err != nil {
		return nil, err
	} else {
		return &save_req, nil
	}
}

func (this *replicator) Start() {
	go this.node.startRaft()
	<-this.snapshotterReadyChan
	go this.readCommits()
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
	raftNode, commitC, errorC, snapshotterReadyC := NewRaftNode(options, store.Backup)
	repl := &replicator{
		node:                 raftNode,
		store:                store,
		confChangeCount:      uint64(0),
		commitChan:           commitC,
		errorChan:            errorC,
		snapshotterReadyChan: snapshotterReadyC,
		replTimeout:          options.ReplTimeout(),
		waiter:               wait.New(),
		idGen:                idutil.NewGenerator(uint16(options.NodeId()), time.Now()),
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
	case res := <-ch:
		repl_res := res.(*internalNexusResponse)
		return repl_res.Err
	case <-child_ctx.Done():
		err := child_ctx.Err()
		this.waiter.Trigger(confChange.ID, &internalNexusResponse{Err: err})
		return err
	}
}

func (this *replicator) readCommits() {
	for data := range this.commitChan {
		if data == nil {
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
			if repl_req, err := unmarshal(data); err != nil {
				log.Fatal(err)
			} else {
				repl_res := internalNexusResponse{}
				// Forward to storage only the regular reqs and not conf change reqs
				if repl_req.Req != nil {
					repl_res.Res, repl_res.Err = this.store.Save(repl_req.Req)
				}
				this.waiter.Trigger(repl_req.ID, &repl_res)
			}
		}
	}
	if err, present := <-this.errorChan; present {
		log.Fatal(err)
	}
}

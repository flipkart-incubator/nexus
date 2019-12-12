package raft

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

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
}

func (this *replicator) Start() {
	go this.node.startRaft()
	<-this.snapshotterReadyChan
	go this.readCommits()
}

func (this *replicator) Replicate(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), this.replTimeout)
	defer cancel()
	if err := this.node.node.Propose(ctx, data); err != nil {
		log.Printf("[WARN] Error occurred while proposing to Raft. Error: %v. Retrying...", err)
		retry_ctx, retry_cancel := context.WithTimeout(context.Background(), this.replTimeout)
		defer retry_cancel()
		if retry_err := this.node.node.Propose(retry_ctx, data); retry_err != nil {
			log.Printf("[ERROR] Unable to propose over Raft. Error: %v.", retry_err)
			return retry_err
		}
	}
	return nil
}

func (this *replicator) ProposeConfigChange(confChange raftpb.ConfChange) {
	confChange.ID = atomic.AddUint64(&this.confChangeCount, 1)
	this.node.node.ProposeConfChange(context.TODO(), confChange)
}

func (this *replicator) Stop() {
	close(this.node.stopc)
	this.store.Close()
}

func NewReplicator(store db.Store, opts ...pkg_raft.Option) (*replicator, error) {
	if store == nil {
		return nil, errors.New("Store must be given")
	}
	if options, err := pkg_raft.NewOptions(opts...); err != nil {
		return nil, err
	} else {
		raftNode, commitC, errorC, snapshotterReadyC := NewRaftNode(options, store.Backup)
		repl := &replicator{node: raftNode, store: store, confChangeCount: uint64(0), commitChan: commitC, errorChan: errorC, snapshotterReadyChan: snapshotterReadyC, replTimeout: options.ReplTimeout()}
		return repl, nil
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
			if err := this.store.Save(data); err != nil {
				log.Fatal(err)
			} else {
				log.Printf("[%d] Successfully saved data", this.node.id)
			}
		}
	}
	if err, present := <-this.errorChan; present {
		log.Fatal(err)
	}
}

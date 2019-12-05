package raft

import (
	"log"

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
}

func (this *replicator) Start() {
	go this.node.startRaft()
	<-this.snapshotterReadyChan
	go this.readCommits()
}

func (this *replicator) Replicate(data interface{}) error {
	return nil
}

func (this *replicator) Stop() {}

func NewReplicator(store db.Store, opts ...pkg_raft.Option) (*replicator, error) {
	if options, err := pkg_raft.NewOptions(opts...); err != nil {
		return nil, err
	} else {
		raftNode, commitC, errorC, snapshotterReadyC := NewRaftNode(options, store.Backup)
		repl := &replicator{node: raftNode, store: store, confChangeCount: uint64(0), commitChan: commitC, errorChan: errorC, snapshotterReadyChan: snapshotterReadyC}
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

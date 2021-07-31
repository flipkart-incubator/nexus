package storage

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
)

// entryStore implements the Storage interface backed by
// an SS table. Specifically we use the implementation
// from Badger for storing entries in sorted manner.
type entryStore struct {
	db *badger.DB
}

func NewEntryStore() (raft.Storage, error) {
	return nil, nil
}

func (es *entryStore) InitialState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (es *entryStore) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	return nil, nil
}

func (es *entryStore) Term(i uint64) (uint64, error) {
	return 0, nil
}

func (es *entryStore) LastIndex() (uint64, error) {
	return 0, nil
}

func (es *entryStore) FirstIndex() (uint64, error) {
	return 0, nil
}

func (es *entryStore) Snapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

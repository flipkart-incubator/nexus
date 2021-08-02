package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger/v3"
	"log"
	"sync"
)

// entryStore implements the Storage interface backed by
// an SS table. Specifically we use the implementation
// from Badger for storing entries in sorted manner.
type entryStore struct {
	sync.Mutex
	db *badger.DB
	hardState pb.HardState
	snapshot  pb.Snapshot
}

func NewEntryStore() (raft.Storage, error) {
	return nil, nil
}

func (es *entryStore) InitialState() (pb.HardState, pb.ConfState, error) {
	return es.hardState, es.snapshot.Metadata.ConfState, nil
}

func (es *entryStore) SetHardState(st pb.HardState) error {
	es.Lock()
	defer es.Unlock()
	es.hardState = st
	return nil
}

func (es *entryStore) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	es.Lock()
	defer es.Unlock()

	beginIndex, err := es.fetchIndexLimit(false)
	if err != nil {
		return nil, err
	}

	if lo <= beginIndex {
		return nil, raft.ErrCompacted
	}

	endIndex, err := es.fetchIndexLimit(true)
	if err != nil {
		return nil, err
	}

	if hi > endIndex {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, endIndex)
	}

	ents, err := es.fetchEntries(lo, hi)
	if err != nil {
		return nil, err
	}
	return limitSize(ents, maxSize), nil
}

func (es *entryStore) Term(i uint64) (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchTermForIndex(i)
}

func (es *entryStore) LastIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchIndexLimit(true)
}

func (es *entryStore) FirstIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchIndexLimit(false)
}

func (es *entryStore) Snapshot() (pb.Snapshot, error) {
	es.Lock()
	defer es.Unlock()
	return es.snapshot, nil
}

func (es *entryStore) ApplySnapshot(snap pb.Snapshot) error {
	es.Lock()
	defer es.Unlock()

	//handle check for old snapshot being applied
	msIndex := es.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return raft.ErrSnapOutOfDate
	}
	es.snapshot = snap
	entries := []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return es.replaceAllEntries(entries)
}

func (es *entryStore) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	es.Lock()
	defer es.Unlock()
	if i <= es.snapshot.Metadata.Index {
		return pb.Snapshot{}, raft.ErrSnapOutOfDate
	}

	lastIdx, err := es.fetchIndexLimit(true)
	if err != nil {
		return pb.Snapshot{}, err
	}
	if i > lastIdx {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, lastIdx)
	}

	term, err := es.fetchTermForIndex(i)
	if err != nil {
		return pb.Snapshot{}, err
	}

	es.snapshot.Metadata.Index = i
	es.snapshot.Metadata.Term = term
	if cs != nil {
		es.snapshot.Metadata.ConfState = *cs
	}
	es.snapshot.Data = data
	return es.snapshot, nil
}

func (es *entryStore) Compact(compactIndex uint64) error {
	es.Lock()
	defer es.Unlock()
	beginIndex, err := es.fetchIndexLimit(false)
	if err != nil {
		return err
	}

	if compactIndex <= beginIndex {
		return raft.ErrCompacted
	}

	endIndex, err := es.fetchIndexLimit(true)
	if err != nil {
		return err
	}

	if compactIndex > endIndex {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, endIndex)
	}
	return es.removeEntriesFrom(compactIndex, true)
}

func (es *entryStore) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	es.Lock()
	defer es.Unlock()

	last := entries[0].Index + uint64(len(entries)) - 1
	firstStoredIndex, err := es.fetchIndexLimit(false)
	if err != nil || last <= firstStoredIndex {
		return err
	}

	lastStoredIndex, err := es.fetchIndexLimit(true)
	if err != nil {
		return err
	}
	if offset := entries[0].Index - lastStoredIndex; offset > 1 {
		log.Panicf("missing log entry [last: %d, append at: %d]",
			lastStoredIndex, entries[0].Index)
	}

	/*
		case 1: truncate compacted entries
		input: 		2-3-4-5
		exist:		  3-4-5
		new_input:	4-5
	 */
	if firstStoredIndex >= entries[0].Index {
		entries = entries[1+firstStoredIndex-entries[0].Index:]
	}

	/*
		case 2: truncate existing entries
		input:		  4'-5'-6'-7'-8'
		exist:		3-4-5
		new_exist:	3-4'-5'-6'-7'-8'
	 */
	if err := es.removeEntriesFrom(entries[0].Index, false); err != nil {
		return err
	}

	/*
		case 3: simple append
		input: 		      4'-5'-6'
		exist:		1-2-3
		new_exist:	1-2-3-4'-5'-6'
	 */
	return es.appendEntries(entries)
}

func limitSize(ents []pb.Entry, maxSize uint64) []pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

func (es *entryStore) fetchIndexLimit(reverse bool) (uint64, error) {
	txn := es.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Reverse:        reverse,
		AllVersions:    false,
	})
	defer it.Close()
	defer txn.Discard()

	if it.Valid() {
		indexBts := it.Item().KeyCopy(nil)
		index := toIndex(indexBts)
		return index, nil
	}

	return 0, raft.ErrUnavailable
}

func (es *entryStore) fetchEntries(lo, hi uint64) ([]pb.Entry, error) {
	loBts, hiBts := toIndexBytes(lo), toIndexBytes(hi)
	txn := es.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchSize:   10,
		PrefetchValues: true,
		Reverse:        false,
		AllVersions:    false,
		InternalAccess: false,
		Prefix:         nil,
		SinceTs:        0,
	})
	defer it.Close()
	defer txn.Discard()

	var result []pb.Entry
	it.Seek(loBts)
	for it.Valid() {
		item := it.Item()
		if entBts, err := item.ValueCopy(nil); err != nil {
			return nil, err
		} else {
			ent := unmarshalEntry(entBts)
			result = append(result, *ent)
		}
		if bytes.Compare(item.Key(), hiBts) == 0 {
			break
		}
	}
	return result, nil
}

func (es *entryStore) fetchTermForIndex(index uint64) (uint64, error) {
	indexBts := toIndexBytes(index)
	txn := es.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(indexBts)
	if err == badger.ErrKeyNotFound {
		return 0, raft.ErrUnavailable
	} else if err != nil {
		return 0, raft.ErrCompacted
	}
	entBts, err := item.ValueCopy(nil)
	if err != nil {
		return 0, nil
	}
	ent := unmarshalEntry(entBts)
	return ent.Term, nil
}

func (es *entryStore) replaceAllEntries(entries []pb.Entry) error {
	if err := es.db.DropAll(); err != nil {
		return err
	}
	return es.appendEntries(entries)
}

func (es *entryStore) appendEntries(entries []pb.Entry) error {
	return es.db.Update(func(txn *badger.Txn) error {
		for _, ent := range entries {
			key := toIndexBytes(ent.Index)
			val := marshalEntry(&ent)
			_ = txn.Set(key, val)
		}
		return nil
	})
}

func (es *entryStore) removeEntriesFrom(index uint64, reverse bool) error {
	return es.db.Update(func(txn *badger.Txn) error {
		idxBts := toIndexBytes(index)
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        reverse,
			AllVersions:    false,
			InternalAccess: false,
			Prefix:         nil,
			SinceTs:        0,
		})
		defer it.Close()
		it.Seek(idxBts)
		if it.Valid() {
			if reverse {
				it.Next()
			}
			for it.Valid() {
				item := it.Item()
				if err := txn.Delete(item.Key()); err != nil {
					return err
				}
			}
			return nil
		} else {
			return raft.ErrUnavailable
		}
	})
}

func toIndex(bts []byte) uint64 {
	return binary.LittleEndian.Uint64(bts)
}

func unmarshalEntry(entByts []byte) *pb.Entry {
	ent := new(pb.Entry)
	pbutil.MustUnmarshal(ent, entByts)
	return ent
}

func marshalEntry(ent *pb.Entry) []byte {
	return pbutil.MustMarshal(ent)
}

func toIndexBytes(index uint64) []byte {
	bts := make([]byte, 8)
	binary.LittleEndian.PutUint64(bts, index)
	return bts
}

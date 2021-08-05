package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger/v3"
	bdgr_opts "github.com/dgraph-io/badger/v3/options"
	"io"
	"log"
	"sync"
)

// EntryStore implements the Storage interface backed by
// an SS table. Specifically we use the implementation
// from Badger for storing entries in sorted manner.
type EntryStore struct {
	sync.Mutex
	io.Closer
	db *badger.DB
	hardState pb.HardState
	snapshot  pb.Snapshot
}

func NewEntryStore(entryDir string) (*EntryStore, error) {
	options := badger.DefaultOptions(entryDir).
		WithCompression(bdgr_opts.None).
		WithSyncWrites(true).
		WithMetricsEnabled(false).
		WithLoggingLevel(badger.ERROR).
		WithDetectConflicts(false)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	return &EntryStore{db: db}, nil
}

func (es *EntryStore) InitialState() (pb.HardState, pb.ConfState, error) {
	return es.hardState, es.snapshot.Metadata.ConfState, nil
}

func (es *EntryStore) SetHardState(st pb.HardState) error {
	es.Lock()
	defer es.Unlock()
	es.hardState = st
	return nil
}

func (es *EntryStore) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
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

	if hi > (endIndex + 1) {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, endIndex)
	}

	ents, err := es.fetchEntries(lo, hi)
	if err != nil {
		return nil, err
	}
	return limitSize(ents, maxSize), nil
}

func (es *EntryStore) Term(i uint64) (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchTermForIndex(i)
}

func (es *EntryStore) LastIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchIndexLimit(true)
}

func (es *EntryStore) FirstIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	return es.fetchIndexLimit(false)
}

func (es *EntryStore) Snapshot() (pb.Snapshot, error) {
	es.Lock()
	defer es.Unlock()
	return es.snapshot, nil
}

func (es *EntryStore) ApplySnapshot(snap pb.Snapshot) error {
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

func (es *EntryStore) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
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

func (es *EntryStore) Compact(compactIndex uint64) error {
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

func (es *EntryStore) Append(entries []pb.Entry) error {
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

func (es *EntryStore) Close() error {
	es.Lock()
	defer es.Unlock()

	return es.db.Close()
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

func (es *EntryStore) fetchIndexLimit(reverse bool) (uint64, error) {
	txn := es.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Reverse:        reverse,
		AllVersions:    false,
	})
	defer txn.Discard()
	defer it.Close()

	if it.Rewind(); it.Valid() {
		indexBts := it.Item().KeyCopy(nil)
		index := toIndex(indexBts)
		return index, nil
	}

	return 0, raft.ErrUnavailable
}

func (es *EntryStore) fetchEntries(lo, hi uint64) ([]pb.Entry, error) {
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
	defer txn.Discard()
	defer it.Close()

	var result []pb.Entry
	for it.Seek(loBts); it.Valid(); it.Next()  {
		item := it.Item()
		if bytes.Compare(item.Key(), hiBts) == 0 {
			break
		}
		if entBts, err := item.ValueCopy(nil); err != nil {
			return nil, err
		} else {
			ent := unmarshalEntry(entBts)
			result = append(result, *ent)
		}
	}
	return result, nil
}

func (es *EntryStore) fetchTermForIndex(index uint64) (uint64, error) {
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

func (es *EntryStore) replaceAllEntries(entries []pb.Entry) error {
	if err := es.db.DropAll(); err != nil {
		return err
	}
	return es.appendEntries(entries)
}

func (es *EntryStore) appendEntries(entries []pb.Entry) error {
	return es.db.Update(func(txn *badger.Txn) error {
		for _, ent := range entries {
			key := toIndexBytes(ent.Index)
			val := marshalEntry(&ent)
			_ = txn.Set(key, val)
		}
		return nil
	})
}

func (es *EntryStore) removeEntriesFrom(index uint64, reverse bool) error {
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
			it.Next()	// skip seeked index
			for ; it.Valid(); it.Next() {
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

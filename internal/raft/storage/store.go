package storage

import (
	"bytes"
	"fmt"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger/v3"
	bdgr_opts "github.com/dgraph-io/badger/v3/options"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
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

func NewTempEntryStore(nodeId uint64) (*EntryStore, error) {
	if entDir, err := ioutil.TempDir(os.TempDir(), fmt.Sprintf("entries_%x", nodeId)); err != nil {
		return nil, err
	} else {
		log.Printf("[Node %x] Creating RAFT entry store inside the folder: %s", nodeId, entDir)
		return NewEntryStore(entDir)
	}
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
	ent := &EntryStore{db: db}
	err = ent.appendEntries(make([]pb.Entry, 1))
	return ent, err
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

	beginIndex, endIndex := es.fetchIndexLimits()
	if lo <= beginIndex {
		return nil, raft.ErrCompacted
	}

	if hi > (endIndex + 1) {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, endIndex)
	}

	ents, err := es.fetchEntries(lo, hi, false)
	if err != nil {
		return nil, err
	}
	return limitSize(ents, maxSize), nil
}

func (es *EntryStore) Term(i uint64) (uint64, error) {
	es.Lock()
	defer es.Unlock()

	firstIdx, lastIdx := es.fetchIndexLimits()
	if i < firstIdx {
		return 0, raft.ErrCompacted
	}

	if i > lastIdx {
		return 0, raft.ErrUnavailable
	}

	return es.fetchTermForIndex(i)
}

func (es *EntryStore) LastIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	_, lastIdx := es.fetchIndexLimits()
	return lastIdx, nil
}

func (es *EntryStore) FirstIndex() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	firstIdx, _ := es.fetchIndexLimits()
	return firstIdx + 1, nil
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

	_, lastIdx := es.fetchIndexLimits()
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
	beginIndex, endIndex := es.fetchIndexLimits()
	if compactIndex <= beginIndex {
		return raft.ErrCompacted
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
	firstStoredIndex, lastStoredIndex := es.fetchIndexLimits()
	if last <= firstStoredIndex {
		return raft.ErrCompacted
	}

	if entries[0].Index > (lastStoredIndex + 1) {
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

func (es *EntryStore) fetchIndexLimits() (min, max uint64) {
	txn := es.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		AllVersions:    false,
	})
	defer txn.Discard()
	defer it.Close()

	once := true
	for it.Rewind(); it.Valid(); it.Next() {
		index := toIndex(it.Item().Key())
		if once {
			min = index
			max = index
			once = false
		} else if index > max {
			max = index
		} else if index < min {
			min = index
		}
	}
	return
}

func (es *EntryStore) fetchEntries(lo, hi uint64, includeHiIndex bool) ([]pb.Entry, error) {
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
	for it.Seek(loBts); it.Valid(); it.Next() {
		item := it.Item()
		if entBts, err := item.ValueCopy(nil); err != nil {
			return nil, err
		} else {
			ent := unmarshalEntry(entBts)
			result = append(result, *ent)
		}
		if bytes.Compare(item.Key(), hiBts) == 0 {
			if !includeHiIndex {
				result = result[:len(result)-1]
			}
			break
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
		return 0, nil
	} else if err != nil {
		return 0, err
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
	txn := es.db.NewTransaction(true)
	defer txn.Discard()

	_, err := txn.Get(toIndexBytes(index))
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		AllVersions:    false,
		InternalAccess: false,
		Prefix:         nil,
		SinceTs:        0,
	})

	for it.Rewind(); it.Valid(); it.Next() {
		itemKey := it.Item().KeyCopy(nil)
		currIdx := toIndex(itemKey)
		if reverse {
			if currIdx < index {
				if err := txn.Delete(itemKey); err != nil {
					return err
				}
			}
		} else {
			if currIdx > index {
				if err := txn.Delete(itemKey); err != nil {
					return err
				}
			}
		}
	}

	it.Close()
	return txn.Commit()
}

func toIndex(bts []byte) uint64 {
	idx, err := strconv.ParseUint(string(bts), 16, 64)
	if err != nil {
		log.Fatalf("unable to convert %s into index, error: %v", string(bts), err)
	}
	return idx
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
	return []byte(strconv.FormatUint(index, 16))
}

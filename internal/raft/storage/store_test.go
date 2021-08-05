package storage

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var store *EntryStore

func TestMain(m *testing.M) {
	dir := filepath.Join(os.TempDir(), "entries")
	if st, err := NewEntryStore(dir); err != nil {
		panic(fmt.Errorf("unable to open entry store, error: %v", err))
	} else {
		store = st
	}
	code := m.Run()
	_ = store.Close()
	os.Exit(code)
}

func TestStorageTerm(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrUnavailable, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	for i, tt := range tests {
		replaceAllEntries(t, ents)
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()

			term, err := store.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
	}
}

func TestStorageEntries(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		replaceAllEntries(t, ents)
		entries, err := store.Entries(tt.lo, tt.hi, tt.maxsize)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	replaceAllEntries(t, ents)

	last, err := store.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 5 {
		t.Errorf("term = %d, want %d", last, 5)
	}

	_ = store.appendEntries([]pb.Entry{{Index: 6, Term: 5}})
	last, err = store.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 6)
	}
}

func TestStorageFirstIndex(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	replaceAllEntries(t, ents)

	first, err := store.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 3 {
		t.Errorf("first = %d, want %d", first, 3)
	}

	_ = store.Compact(4)
	first, err = store.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}
}

func TestStorageCompact(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, raft.ErrCompacted, 3, 3, 3},
		{3, raft.ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}

	for i, tt := range tests {
		replaceAllEntries(t, ents)
		err := store.Compact(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		firstIndex, _ := store.FirstIndex()
		firstTerm, _ := store.Term(firstIndex)
		if firstIndex != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, firstIndex, tt.windex)
		}
		if firstTerm != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, firstTerm, tt.wterm)
		}
		lastIndex, _ := store.LastIndex()
		allEnts, _ := store.fetchEntries(firstIndex, lastIndex, true)
		numEnts := len(allEnts)
		if numEnts != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, numEnts, tt.wlen)
		}
	}
}

func TestStorageCreateSnapshot(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.ConfState{Nodes: []uint64{1, 2, 3}}
	data := []byte("data")

	tests := []struct {
		i uint64

		werr  error
		wsnap pb.Snapshot
	}{
		{4, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
		{5, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
	}

	for i, tt := range tests {
		replaceAllEntries(t, ents)
		snap, err := store.CreateSnapshot(tt.i, cs, data)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(snap, tt.wsnap) {
			t.Errorf("#%d: snap = %+v, want %+v", i, snap, tt.wsnap)
		}
	}
}

func TestStorageAppend(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		entries []pb.Entry

		werr     error
		wentries []pb.Entry
	}{
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// truncate the existing entries and append
		{
			[]pb.Entry{{Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			[]pb.Entry{{Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}

	for i, tt := range tests {
		replaceAllEntries(t, ents)
		err := store.Append(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}

		firstIndex, _ := store.FirstIndex()
		lastIndex, _ := store.LastIndex()
		allEnts, _ := store.fetchEntries(firstIndex, lastIndex, true)
		if !reflect.DeepEqual(allEnts, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, allEnts, tt.wentries)
		}
	}
}

func TestStorageApplySnapshot(t *testing.T) {
	cs := &pb.ConfState{Nodes: []uint64{1, 2, 3}}
	data := []byte("data")

	tests := []pb.Snapshot{{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}},
		{Data: data, Metadata: pb.SnapshotMetadata{Index: 3, Term: 3, ConfState: *cs}},
	}

	resetStore(t)

	//Apply Snapshot successful
	i := 0
	tt := tests[i]
	err := store.ApplySnapshot(tt)
	if err != nil {
		t.Errorf("#%d: err = %v, want %v", i, err, nil)
	}

	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = store.ApplySnapshot(tt)
	if err != raft.ErrSnapOutOfDate {
		t.Errorf("#%d: err = %v, want %v", i, err, raft.ErrSnapOutOfDate)
	}
}

func resetStore(t *testing.T) {
	replaceAllEntries(t, nil)
	store.snapshot = pb.Snapshot{}
	store.hardState = pb.HardState{}
}

func replaceAllEntries(t *testing.T, ents []pb.Entry) {
	err := store.replaceAllEntries(ents)
	if err != nil {
		t.Fatalf("unable to replace all entries, error: %v", err)
	}
}

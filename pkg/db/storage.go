package db

import "io"

type SnapshotState struct {
	SnapshotIndex uint64
	AppliedIndex  uint64
}

type RaftEntry struct {
	Term  uint64
	Index uint64
}

type Store interface {
	io.Closer
	GetLastAppliedEntry() (RaftEntry, error)
	Save(RaftEntry, []byte) ([]byte, error)
	Load([]byte) ([]byte, error)

	Backup(SnapshotState) (io.ReadCloser, error)
	Restore(io.ReadCloser) error
}

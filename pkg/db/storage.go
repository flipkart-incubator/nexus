package db

import "io"

type SnapshotState struct {
	SnapshotIndex uint64
	AppliedIndex  uint64
}

type Store interface {
	io.Closer
	Save([]byte) ([]byte, error)
	Load([]byte) ([]byte, error)

	Backup(SnapshotState) ([]byte, error)
	Restore([]byte) error
}

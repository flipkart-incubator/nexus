package db

import "io"

type Store interface {
	io.Closer
	Save([]byte) ([]byte, error)
	Load([]byte) ([]byte, error)

	Backup() ([]byte, error)
	Restore([]byte) error
}

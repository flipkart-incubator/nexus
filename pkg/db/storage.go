package db

import "io"

type Store interface {
	io.Closer
	Save([]byte) error

	Backup() ([]byte, error)
	Restore([]byte) error
}

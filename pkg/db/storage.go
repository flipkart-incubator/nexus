package db

import "io"

type Store interface {
	io.Closer
	Save(interface{}) error

	Backup() ([]byte, error)
	Restore([]byte) error
}

package db

type Store interface {
	Save(interface{}) error
	Delete(interface{}) error

	Backup() ([]byte, error)
	Restore([]byte) error
}

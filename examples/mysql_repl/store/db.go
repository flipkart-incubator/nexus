package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"log"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type SaveRequest struct {
	StmtTmpl string
	Params   map[string]interface{}
}

func FromBytes(data []byte) (*SaveRequest, error) {
	save_req := SaveRequest{}
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&save_req); err != nil {
		return nil, err
	} else {
		return &save_req, nil
	}
}

func (this *SaveRequest) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(this); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type mysqlStore struct {
	db *sql.DB
}

func NewMySQLDB(datasourceName string) (*mysqlStore, error) {
	if db, err := sql.Open("mysql", datasourceName); err != nil {
		return nil, err
	} else {
		if err := db.Ping(); err != nil {
			return nil, err
		} else {
			return &mysqlStore{db}, nil
		}
	}
}

func (this *mysqlStore) Close() error {
	return this.db.Close()
}

const txTimeout = 5 * time.Second // TODO: Should be configurable

func (this *mysqlStore) save(sqlStmt string) error {
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	// TODO: Isolation level can be part of the SaveRequest itself ?
	if tx, err := this.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err != nil {
		return err
	} else {
		log.Printf("About to execute SQL: %s", sqlStmt)
		if _, err := tx.ExecContext(ctx, sqlStmt); err != nil {
			tx.Rollback() // TODO: Ignore rollback errors ?
			return err
		} else {
			log.Printf("Successfully executed SQL: %s", sqlStmt)
			return tx.Commit()
		}
	}
}

func (this *mysqlStore) Save(data []byte) ([]byte, error) {
	if save_req, err := FromBytes(data); err != nil {
		return nil, err
	} else {
		sql := save_req.StmtTmpl
		if tmpl, err := template.New("sql_tmpl").Parse(sql); err != nil {
			return nil, err
		} else {
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, save_req.Params); err != nil {
				return nil, err
			} else {
				return nil, this.save(buf.String())
			}
		}
	}
}

func (this *mysqlStore) Backup() ([]byte, error) {
	return nil, nil
}

func (this *mysqlStore) Restore(data []byte) error {
	return nil
}

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"log"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type LoadRequest struct {
	StmtTmpl string
	Params   map[string]interface{}
}

func (this *LoadRequest) FromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(this); err != nil {
		return err
	}
	return nil
}

func (this *LoadRequest) ToBytes() ([]byte, error) {
	return encode(this)
}

type SaveRequest struct {
	StmtTmpl string
	Params   map[string]interface{}
}

func (this *SaveRequest) FromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(this); err != nil {
		return err
	}
	return nil
}

func (this *SaveRequest) ToBytes() ([]byte, error) {
	return encode(this)
}

func encode(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(obj); err != nil {
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

// TODO: implement this correctly
func (this *mysqlStore) GetLastAppliedEntry() (db.RaftEntry, error) {
	return db.RaftEntry{}, errors.New("not implemented")
}

const txTimeout = 20 * time.Second // TODO: Should be configurable

func (this *mysqlStore) load(ctx context.Context, sqlStmt string) (*sql.Rows, error) {
	// TODO: Isolation level can be part of the LoadRequest itself ?
	if tx, err := this.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}); err != nil {
		return nil, err
	} else {
		log.Printf("About to execute SQL: %s", sqlStmt)
		return tx.QueryContext(ctx, sqlStmt)
	}
}

func (this *mysqlStore) save(sqlStmt string) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	// TODO: Isolation level can be part of the SaveRequest itself ?
	if tx, err := this.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err != nil {
		return nil, err
	} else {
		log.Printf("About to execute SQL: %s", sqlStmt)
		if res, err := tx.ExecContext(ctx, sqlStmt); err != nil {
			tx.Rollback()
			return nil, err
		} else {
			log.Printf("Successfully executed SQL: %s", sqlStmt)
			return res, tx.Commit()
		}
	}
}

func (this *mysqlStore) Load(data []byte) ([]byte, error) {
	var load_req LoadRequest
	if err := load_req.FromBytes(data); err != nil {
		return nil, err
	} else {
		sql := load_req.StmtTmpl
		if tmpl, err := template.New("sql_tmpl").Parse(sql); err != nil {
			return nil, err
		} else {
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, load_req.Params); err != nil {
				return nil, err
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
				defer cancel()
				if rows, err := this.load(ctx, buf.String()); err != nil {
					return nil, err
				} else {
					defer rows.Close()
					var results []map[string]interface{}
					cols, _ := rows.Columns()
					for rows.Next() {
						columns := make([]string, len(cols))
						columnPointers := make([]interface{}, len(cols))
						for i, _ := range columns {
							columnPointers[i] = &columns[i]
						}
						if err := rows.Scan(columnPointers...); err != nil {
							return nil, err
						}
						row := make(map[string]interface{})
						for i, colName := range cols {
							val := columnPointers[i].(*string)
							row[colName] = *val
						}
						results = append(results, row)
					}
					return []byte(fmt.Sprintf("%v", results)), rows.Err()
				}
			}
		}
	}
}

func (this *mysqlStore) Save(_ db.RaftEntry, data []byte) ([]byte, error) {
	var saveReq SaveRequest
	if err := saveReq.FromBytes(data); err != nil {
		return nil, err
	} else {
		sqlTmpl := saveReq.StmtTmpl
		if tmpl, err := template.New("sql_tmpl").Parse(sqlTmpl); err != nil {
			return nil, err
		} else {
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, saveReq.Params); err != nil {
				return nil, err
			} else {
				if res, err := this.save(buf.String()); err != nil {
					return nil, err
				} else {
					lastInsertId, _ := res.LastInsertId()
					rowsAffected, _ := res.RowsAffected()
					msg := fmt.Sprintf("{ LastInsertId: %v, RowsAffected: %v }", lastInsertId, rowsAffected)
					return []byte(msg), nil
				}
			}
		}
	}
}

func (this *mysqlStore) Backup(_ db.SnapshotState) ([]byte, error) {
	return nil, nil
}

func (this *mysqlStore) Restore(data []byte) error {
	return nil
}

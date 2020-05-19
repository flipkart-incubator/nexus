package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
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

const txTimeout = 5 * time.Second // TODO: Should be configurable

func (this *mysqlStore) load(sqlStmt string) (*sql.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	// TODO: Isolation level can be part of the SaveRequest itself ?
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
				if rows, err := this.load(buf.String()); err != nil {
					return nil, err
				} else {
					defer rows.Close()
					var results []map[string]interface{}
					cols, _ := rows.Columns()
					for rows.Next() {
						columns := make([]interface{}, len(cols))
						columnPointers := make([]interface{}, len(cols))
						for i, _ := range columns {
							columnPointers[i] = &columns[i]
						}
						if err := rows.Scan(columnPointers...); err != nil {
							return nil, err
						}
						row := make(map[string]interface{})
						for i, colName := range cols {
							val := columnPointers[i].(*interface{})
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

func (this *mysqlStore) Save(data []byte) ([]byte, error) {
	var save_req SaveRequest
	if err := save_req.FromBytes(data); err != nil {
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
				if res, err := this.save(buf.String()); err != nil {
					return nil, err
				} else {
					return []byte(fmt.Sprintf("%#v", res)), nil
				}
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

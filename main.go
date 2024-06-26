// The fastdb package creates a pair of sqlite3 clients which
// are optimised for performance: a read-only client which supports
// parallel operations, and a read-write client which only supports
// one write at a time.
package fastdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"runtime"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Time is used to store timestamps as INT in SQLite
type Time int64

func (t *Time) Scan(val any) (err error) {
	switch v := val.(type) {
	case int64:
		*t = Time(v)
		return nil
	case string:
		tt, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return err
		}
		*t = Time(tt.UnixMilli())
		return nil
	default:
		return fmt.Errorf("Time.Scan: Unsupported type: %T", v)
	}
}

func (t *Time) Value() (driver.Value, error) {
	return *t, nil
}

func setupSqlite(db *sql.DB) (err error) {
	pragmas := []string{
		"temp_store = memory",
	}

	for _, pragma := range pragmas {
		_, err = db.Exec("PRAGMA " + pragma)
		if err != nil {
			return
		}
	}

	return nil
}

type rw struct {
	reader *sql.DB
	writer *sql.DB
}

type FastDB interface {
	Close() error
	Reader() *sql.DB
	Writer() *sql.DB
}

// Close will close the underlying sqlite3 clients, returning
// any resultant error.
func (r *rw) Close() error {
	if r.writer != nil {
		if err := r.writer.Close(); err != nil {
			return err
		}
	}
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Reader returns a read-only sqlite3 client
func (r *rw) Reader() *sql.DB {
	return r.reader
}

// Writer returns a read-write sqlite3 client
func (r *rw) Writer() *sql.DB {
	return r.writer
}

// Open creates a FastDB wrapper around the sqlite3 database
// located at filename. If there is a problem opening either
// of the underlying clients, that error is returned.
func Open(filename string) (*rw, error) {
	connectionUrlParams := make(url.Values)
	connectionUrlParams.Add("_txlock", "immediate")
	connectionUrlParams.Add("_journal_mode", "WAL")
	connectionUrlParams.Add("_busy_timeout", "5000")
	connectionUrlParams.Add("_synchronous", "NORMAL")
	connectionUrlParams.Add("_cache_size", "1000000000")
	connectionUrlParams.Add("_foreign_keys", "true")
	connectionUrl := fmt.Sprintf("file:%v?", filename) + connectionUrlParams.Encode()

	r := rw{}

	writeDB, err := sql.Open("sqlite3", connectionUrl)
	if err != nil {
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	err = setupSqlite(writeDB)
	if err != nil {
		return nil, err
	}
	r.writer = writeDB

	readDB, err := sql.Open("sqlite3", connectionUrl)
	if err != nil {
		return nil, err
	}
	readDB.SetMaxOpenConns(max(4, runtime.NumCPU()))
	err = setupSqlite(readDB)
	if err != nil {
		return nil, err
	}
	r.reader = readDB

	return &r, nil
}

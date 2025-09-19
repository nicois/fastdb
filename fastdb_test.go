package fastdb_test

import (
	"fmt"
	"testing"

	"github.com/nicois/fastdb"
	"github.com/stretchr/testify/require"
)

const CREATE string = `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, bar TEXT)`

func TestWriteThenRead(t *testing.T) {
	tempDir := t.TempDir()
	db, err := fastdb.Open(fmt.Sprintf("%v/db.sqlite3", tempDir))
	require.NoError(t, err)

	reader := db.Reader()
	writer := db.Writer()

	_, err = writer.Exec(CREATE)
	require.NoError(t, err)

	result, err := writer.Exec("INSERT INTO foo (bar) VALUES (?)", "baz")
	require.NoError(t, err)
	pkey, err := result.LastInsertId()
	require.NoError(t, err)

	var s string
	row := reader.QueryRow(`SELECT bar FROM foo WHERE id=?`, pkey)
	require.NoError(t, row.Scan(&s))
	require.Equal(t, "baz", s)
}

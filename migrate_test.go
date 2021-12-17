package migrator

import (
	"database/sql"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

func TestReadMigrations(t *testing.T) {
	dir := fstest.MapFS{
		"skip":          {Data: []byte("this should be skipped")},
		"dir":           {Mode: fs.ModeDir},
		"0002.sql":      {Data: []byte("sql")},
		"0001_init.sql": {Data: []byte("sql")},
	}
	if entries, err := readMigrations(dir); err != nil {
		assert.NoError(t, err)
	} else {
		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "0001_init.sql", entries[0].Name())
		assert.Equal(t, "0002.sql", entries[1].Name())
	}
}

func TestRunSuccess(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)

	// Initial migration
	dir := fstest.MapFS{
		"0001.sql": {Data: []byte("create table test(id number)")},
		"0002.sql": {Data: []byte("insert into test values(1)")},
	}

	ver, err := Run(db, dir)
	assert.NoError(t, err)
	assert.Equal(t, 2, ver)

	// Ensure version is written to db
	var currentVersion int
	db.QueryRow("PRAGMA user_version").Scan(&currentVersion)
	assert.NoError(t, err)
	assert.Equal(t, 2, currentVersion)

	// Add another
	dir = fstest.MapFS{
		"0001.sql": {Data: []byte("this should not be run")},
		"0002.sql": {Data: []byte("so should this")},
		"0003.sql": {Data: []byte("insert into test values(2)")},
	}

	ver, err = Run(db, dir)
	assert.NoError(t, err)
	assert.Equal(t, 3, ver)

	var count int
	err = db.QueryRow("select count(id) from test").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// Run again
	dir = fstest.MapFS{
		"0001.sql": {Data: []byte("this should not be run")},
		"0002.sql": {Data: []byte("so should this")},
		"0003.sql": {Data: []byte("insert into test values(2)")},
	}

	ver, err = Run(db, dir)
	assert.NoError(t, err)
	assert.Equal(t, 3, ver)
}

func TestRunError(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)

	// Initial migration
	dir := fstest.MapFS{
		"0001.sql": {Data: []byte("create table test(id number)")},
		"0002.sql": {Data: []byte("invalid sql, this should fail")},
	}

	ver, err := Run(db, dir)
	assert.Error(t, err)
	assert.Equal(t, -1, ver)

	var id int
	err = db.QueryRow("select id from test").Scan(&id)
	assert.Error(t, err)
	assert.EqualError(t, err, "SQL logic error: no such table: test (1)")
}

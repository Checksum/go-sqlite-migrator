package migrator

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"database/sql"
)

const invalid = -1

var logger = log.New(os.Stderr, "[sqlite-migrator] ", log.LstdFlags)

func readMigrations(dir fs.FS) ([]fs.DirEntry, error) {
	var err error
	var migrations []fs.DirEntry

	// Filter out sql files
	if err = fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".sql" {
			migrations = append(migrations, d)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return migrations, nil
}

func Run(db *sql.DB, dir fs.FS) (int, error) {
	migrations, err := readMigrations(dir)
	if err != nil {
		return invalid, err
	}
	latestVersion := len(migrations)
	if latestVersion == 0 {
		return invalid, fmt.Errorf("no migrations to run, do you have *.sql files in the target directory?")
	}

	// Read current version
	var currentVersion int
	err = db.QueryRow("PRAGMA user_version").Scan(&currentVersion)
	if err != nil || currentVersion < 0 || currentVersion > latestVersion {
		return invalid, fmt.Errorf("current version is invalid or missing in the migrations list, cannot proceed")
	}

	logger.Printf("current DB version: %d, latest version: %d", currentVersion, latestVersion)

	if currentVersion == latestVersion {
		logger.Printf("DB upto date!")
		return latestVersion, nil
	}

	// Run migrations
	var tx *sql.Tx
	if tx, err = db.Begin(); err != nil {
		return invalid, err
	}

	for _, migration := range migrations[currentVersion:] {
		name := migration.Name()
		logger.Printf("running migration %s", name)
		if sql, err := fs.ReadFile(dir, name); err != nil {
			_ = tx.Rollback()
			return invalid, fmt.Errorf("could not read migration file %s", name)
		} else {
			if _, err = tx.Exec(string(sql)); err != nil {
				_ = tx.Rollback()
				return invalid, fmt.Errorf("error executing migration %s: %s", name, err)
			}
		}
	}

	// Update current version and commit tx
	logger.Printf("writing new DB version: %d", latestVersion)
	if _, err = tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", latestVersion)); err != nil {
		_ = tx.Rollback()
		return invalid, fmt.Errorf("could not set user_version to %d: %s", latestVersion, err)
	}
	if err = tx.Commit(); err != nil {
		return invalid, err
	}

	return latestVersion, nil
}

func FromDir(db *sql.DB, dir string) (int, error) {
	return Run(db, os.DirFS(dir))
}

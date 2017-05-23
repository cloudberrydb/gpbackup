package testutils

/*
 * This file contains test structs and functions used in unit tests via dependency injection.
 */

import (
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type TestDriver struct {
	DBExists bool
	DB       *sqlx.DB
	DBName   string
}

func (driver TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.DBExists {
		return driver.DB, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Database %s does not exist", driver.DBName))
	}
}

type TestResult struct {
	Rows int64
}

func (result TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (result TestResult) RowsAffected() (int64, error) {
	return result.Rows, nil
}

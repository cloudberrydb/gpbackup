package utils

/*
 * This file contains structs and functions used for unit testing other structs
 * and functions in this directory via dependency injection.
 */

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	System = InitializeSystemFunctions()
)

/*
 * Structs and functions for testing database functions
 */

type DBDriver interface {
	Connect(driverName string, dataSourceName string) (*sqlx.DB, error)
}

type GPDBDriver struct {
}

type TestDriver struct {
	DBExists bool
	DB       *sqlx.DB
	DBName   string
}

type TestResult struct {
	Rows int64
}

func (driver GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}

func (driver TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.DBExists {
		return driver.DB, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Database %s does not exist", driver.DBName))
	}
}

func (result TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (result TestResult) RowsAffected() (int64, error) {
	return result.Rows, nil
}

/*
 * SystemFunctions holds function pointers for built-in functions that will need
 * to be mocked out for unit testing.  All built-in functions manipulating the
 * filesystem, shell, or environment should ideally be called through a function
 * pointer in System (the global SystemFunctions variable) instead of being called
 * directly.
 */

type SystemFunctions struct {
	Create func(name string) (*os.File, error)
	CurrentUser func() (*user.User, error)
	Getenv func(key string) string
	Getpid func() int
	Hostname func() (string, error)
	MkdirAll func(path string, perm os.FileMode) error
	Now func() time.Time
	Stat func(name string) (os.FileInfo, error)
}

func InitializeSystemFunctions() *SystemFunctions {
	return &SystemFunctions{
		Create: os.Create,
		CurrentUser: user.Current,
		Getenv: os.Getenv,
		Getpid: os.Getpid,
		Hostname: os.Hostname,
		MkdirAll: os.MkdirAll,
		Now: time.Now,
		Stat: os.Stat,
	}
}

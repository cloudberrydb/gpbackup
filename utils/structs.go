package utils

/*
 * This file contains structs and functions used as entry points for
 * unit testing via dependency injection.
 */

import (
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

func (driver GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}

/*
 * SystemFunctions holds function pointers for built-in functions that will need
 * to be mocked out for unit testing.  All built-in functions manipulating the
 * filesystem, shell, or environment should ideally be called through a function
 * pointer in System (the global SystemFunctions variable) instead of being called
 * directly.
 */

type SystemFunctions struct {
	CurrentUser func() (*user.User, error)
	Getenv      func(key string) string
	Getpid      func() int
	Hostname    func() (string, error)
	IsNotExist  func(err error) bool
	MkdirAll    func(path string, perm os.FileMode) error
	Now         func() time.Time
	OpenFile    func(name string, flag int, perm os.FileMode) (*os.File, error)
	Stat        func(name string) (os.FileInfo, error)
}

func InitializeSystemFunctions() *SystemFunctions {
	return &SystemFunctions{
		CurrentUser: user.Current,
		Getenv:      os.Getenv,
		Getpid:      os.Getpid,
		Hostname:    os.Hostname,
		IsNotExist:  os.IsNotExist,
		MkdirAll:    os.MkdirAll,
		Now:         time.Now,
		OpenFile:    os.OpenFile,
		Stat:        os.Stat,
	}
}

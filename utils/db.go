package utils

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Need driver for postgres
	"github.com/pkg/errors"
)

const MINIMUM_GPDB4_VERSION = "4.3.17"
const MINIMUM_GPDB5_VERSION = "5.1.0"

type DBConn struct {
	Conn    *sqlx.DB
	Driver  DBDriver
	User    string
	DBName  string
	Host    string
	Port    int
	Tx      *sqlx.Tx
	Version GPDBVersion
}

func NewDBConn(dbname string) *DBConn {
	username := ""
	host := ""
	port := 0

	currentUser, _, currentHost := GetUserAndHostInfo()
	username = TryEnv("PGUSER", currentUser)
	if dbname == "" {
		logger.Fatal(errors.New("No database provided"), "")
	}
	host = TryEnv("PGHOST", currentHost)
	port, _ = strconv.Atoi(TryEnv("PGPORT", "5432"))

	return &DBConn{
		Conn:    nil,
		Driver:  GPDBDriver{},
		User:    username,
		DBName:  dbname,
		Host:    host,
		Port:    port,
		Tx:      nil,
		Version: GPDBVersion{},
	}
}

/*
 * Wrapper functions for built-in sqlx and database/sql functionality; they will
 * automatically execute the query as part of an existing transaction if one is
 * in progress, to ensure that the whole backup process occurs in one transaction
 * without requiring that to be ensured at the call site.
 */

func (dbconn *DBConn) Begin() {
	if dbconn.Tx != nil {
		logger.Fatal(errors.New("Cannot begin transaction; there is already a transaction in progress"), "")
	}
	var err error
	dbconn.Tx, err = dbconn.Conn.Beginx()
	CheckError(err)
	/*
	 * This uses a SERIALIZABLE transaction so the backup can effectively take a
	 * "snapshot" of the database via MVCC, to keep backups consistent without
	 * requiring a pg_class lock.
	 */
	_, err = dbconn.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	CheckError(err)
}

func (dbconn *DBConn) Close() {
	if dbconn.Conn != nil {
		dbconn.Conn.Close()
	}
}

func (dbconn *DBConn) Commit() {
	if dbconn.Tx == nil {
		logger.Fatal(errors.New("Cannot commit transaction; there is no transaction in progress"), "")
	}
	var err error
	err = dbconn.Tx.Commit()
	CheckError(err)
	dbconn.Tx = nil
}

func (dbconn *DBConn) Connect() {
	dbname := escapeConnectionParam(dbconn.DBName)
	user := escapeConnectionParam(dbconn.User)
	connStr := fmt.Sprintf(`user='%s' dbname='%s' host=%s port=%d sslmode=disable`, user, dbname, dbconn.Host, dbconn.Port)
	var err error
	dbconn.Conn, err = dbconn.Driver.Connect("postgres", connStr)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			if strings.Contains(err.Error(), "pq: role") {
				logger.Fatal(errors.Errorf(`Role "%s" does not exist, exiting`, dbconn.User), "")
			} else if strings.Contains(err.Error(), "pq: database") {
				logger.Fatal(errors.Errorf(`Database "%s" does not exist, exiting`, dbconn.DBName), "")
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			logger.Fatal(errors.Errorf(`could not connect to server: Connection refused
	Is the server running on host "%s" and accepting
	TCP/IP connections on port %d?`, dbconn.Host, dbconn.Port), "")
		}
	}
	CheckError(err)
}

func (dbconn *DBConn) Exec(query string) (sql.Result, error) {
	if dbconn.Tx != nil {
		return dbconn.Tx.Exec(query)
	}
	return dbconn.Conn.Exec(query)
}

func (dbconn *DBConn) Get(destination interface{}, query string) error {
	if dbconn.Tx != nil {
		return dbconn.Tx.Get(destination, query)
	}
	return dbconn.Conn.Get(destination, query)
}

func (dbconn *DBConn) Select(destination interface{}, query string) error {
	if dbconn.Tx != nil {
		return dbconn.Tx.Select(destination, query)
	}
	return dbconn.Conn.Select(destination, query)
}

/*
 * Other useful/helper functions involving DBConn
 */

func escapeConnectionParam(param string) string {
	param = strings.Replace(param, `\`, `\\`, -1)
	param = strings.Replace(param, `'`, `\'`, -1)
	return param
}

func (dbconn *DBConn) GetDBSize() string {
	size := struct{ DBSize string }{}
	sizeQuery := fmt.Sprintf("SELECT pg_size_pretty(sodddatsize) as dbsize FROM gp_toolkit.gp_size_of_database WHERE sodddatname=E'%s'", escapeConnectionParam(dbconn.DBName))
	err := dbconn.Get(&size, sizeQuery)
	CheckError(err)
	return size.DBSize
}

func (dbconn *DBConn) SetDatabaseVersion() {
	dbconn.Version.Initialize(dbconn)
	dbconn.validateGPDBVersionCompatibility()
}

func (dbconn *DBConn) validateGPDBVersionCompatibility() {
	if dbconn.Version.Before(MINIMUM_GPDB4_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s.0 or later.`, dbconn.Version.VersionString, MINIMUM_GPDB4_VERSION), "")
	} else if dbconn.Version.Is("5") && dbconn.Version.Before(MINIMUM_GPDB5_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s or later.`, dbconn.Version.VersionString, MINIMUM_GPDB5_VERSION), "")
	}
}

func (dbconn *DBConn) ExecuteAllStatements(statements []StatementWithType) {
	for _, statement := range statements {
		_, err := dbconn.Exec(statement.Statement)
		CheckErrorForQuery(err, statement.Statement)
	}
}

func (dbconn *DBConn) ExecuteAllStatementsMatching(statements []StatementWithType, objectTypes ...string) {
	shouldExecute := make(map[string]bool, len(objectTypes))
	for _, obj := range objectTypes {
		shouldExecute[obj] = true
	}
	for _, statement := range statements {
		if shouldExecute[statement.ObjectType] {
			_, err := dbconn.Exec(statement.Statement)
			CheckErrorForQuery(err, statement.Statement)
		}
	}
}

func (dbconn *DBConn) ExecuteAllStatementsExcept(statements []StatementWithType, objectTypes ...string) {
	shouldNotExecute := make(map[string]bool, len(objectTypes))
	for _, obj := range objectTypes {
		shouldNotExecute[obj] = true
	}
	for _, statement := range statements {
		if !shouldNotExecute[statement.ObjectType] {
			_, err := dbconn.Exec(statement.Statement)
			CheckErrorForQuery(err, statement.Statement)
		}
	}
}

func CheckErrorForQuery(err error, statement string) {
	if err != nil {
		logger.Verbose("Attempted to execute statement:\n%s\n", statement)
		logger.Fatal(err, "Failed to execute statement; see log file %s for details.  Error was", logger.GetLogFilePath())
	}
}

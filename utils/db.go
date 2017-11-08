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
	"sync"
	"time"

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

/*
 * Helper functions for executing SQL statements
 */

/*
 * The shouldExec function should accept a StatementWithType and return whether
 * it should be executed.  This allows the ExecuteAllStatements*() functions
 * below to filter statements before execution.
 */
func (dbconn *DBConn) executeStatement(statement StatementWithType, shouldExec func(statement StatementWithType) bool) {
	if shouldExec(statement) {
		_, err := dbconn.Exec(statement.Statement)
		CheckErrorForQuery(err, statement.Statement)
	}
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel; the value of numJobs passed in should either be
 * set to 1 (to execute everything in series) or the value of the numJobs flag
 * (so the number of goroutines match the available database connections).
 */
func (dbconn *DBConn) executeStatements(statements []StatementWithType, numJobs int, shouldExec func(statement StatementWithType) bool, showProgressBar bool) {
	progressBar := NewProgressBar(len(statements), "Objects restored: ", showProgressBar)
	progressBar.Start()
	if numJobs == 1 {
		for _, statement := range statements {
			dbconn.executeStatement(statement, shouldExec)
			progressBar.Increment()
		}
	} else {
		tasks := make(chan StatementWithType, len(statements))
		var workerPool sync.WaitGroup
		for i := 0; i < numJobs; i++ {
			workerPool.Add(1)
			go func(i int) {
				for statement := range tasks {
					dbconn.executeStatement(statement, shouldExec)
					progressBar.Increment()
				}
				workerPool.Done()
			}(i)
		}
		for _, statement := range statements {
			tasks <- statement
			/*
			 * Attempting to execute certain statements such as CREATE INDEX on the same table
			 * at the same time can cause a deadlock due to conflicting Access Exclusive locks,
			 * so we add a small delay between statements to avoid the issue.
			 */
			time.Sleep(20 * time.Millisecond)
		}
		close(tasks)
		workerPool.Wait()
	}
	progressBar.Finish()
}

func (dbconn *DBConn) ExecuteAllStatements(statements []StatementWithType, numJobs int, showProgressBar bool) {
	shouldExec := func(statement StatementWithType) bool {
		return true
	}
	dbconn.executeStatements(statements, numJobs, shouldExec, showProgressBar)
}

func (dbconn *DBConn) ExecuteAllStatementsExcept(statements []StatementWithType, numJobs int, showProgressBar bool, objectTypes ...string) {
	shouldNotExecute := make(map[string]bool, len(objectTypes))
	for _, obj := range objectTypes {
		shouldNotExecute[obj] = true
	}
	shouldExec := func(statement StatementWithType) bool {
		return !shouldNotExecute[statement.ObjectType]
	}
	dbconn.executeStatements(statements, numJobs, shouldExec, showProgressBar)
}

func CheckErrorForQuery(err error, statement string) {
	if err != nil {
		logger.Verbose("Attempted to execute statement:\n%s\n", statement)
		logger.Fatal(err, "Failed to execute statement; see log file %s for details.  Error was", logger.GetLogFilePath())
	}
}

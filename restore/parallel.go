package restore

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

var (
	executeInParallel bool
)

func setParallelRestore() {
	executeInParallel = true
	connection.Conn.SetMaxOpenConns(*numJobs)
	connection.Conn.SetMaxIdleConns(*numJobs)
}

func setSerialRestore() {
	executeInParallel = false
	connection.Conn.SetMaxOpenConns(1)
	connection.Conn.SetMaxIdleConns(1)
}

/*
 * The shouldExec function should accept a StatementWithType and return whether
 * it should be executed.  This allows the ExecuteAllStatements*() functions
 * below to filter statements before execution.
 *
 * The return value for this function is the number of errors encountered, not
 * an error code.
 */
func executeStatement(statement utils.StatementWithType, showProgressBar bool, shouldExec func(statement utils.StatementWithType) bool) uint32 {
	if shouldExec(statement) {
		_, err := connection.Exec(statement.Statement)
		if err != nil {
			if showProgressBar {
				fmt.Println() // Move error message to its own line, since the cursor is currently at the end of the progress bar
			}
			logger.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if *onErrorContinue {
				return 1
			}
			logger.Fatal(errors.Errorf("%s; see log file %s for details.", err.Error(), logger.GetLogFilePath()), "Failed to execute statement:")
		}
	}
	return 0
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func executeStatements(statements []utils.StatementWithType, showProgressBar bool, objectsTitle string, shouldExec func(statement utils.StatementWithType) bool) {
	var numErrors uint32
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
	if !executeInParallel {
		for _, statement := range statements {
			atomic.AddUint32(&numErrors, executeStatement(statement, showProgressBar, shouldExec))
			progressBar.Increment()
		}
	} else {
		tasks := make(chan utils.StatementWithType, len(statements))
		var workerPool sync.WaitGroup
		for i := 0; i < *numJobs; i++ {
			workerPool.Add(1)
			go func() {
				for statement := range tasks {
					atomic.AddUint32(&numErrors, executeStatement(statement, showProgressBar, shouldExec))
					progressBar.Increment()
				}
				workerPool.Done()
			}()
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
	if numErrors > 0 {
		logger.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, logger.GetLogFilePath())
	}
}

func ExecuteAllStatements(statements []utils.StatementWithType, objectsTitle string, showProgressBar bool) {
	shouldExec := func(statement utils.StatementWithType) bool {
		return true
	}
	executeStatements(statements, showProgressBar, objectsTitle, shouldExec)
}

func ExecuteAllStatementsExcept(statements []utils.StatementWithType, objectsTitle string, showProgressBar bool, objectTypes ...string) {
	shouldNotExecute := make(map[string]bool, len(objectTypes))
	for _, obj := range objectTypes {
		shouldNotExecute[obj] = true
	}
	shouldExec := func(statement utils.StatementWithType) bool {
		return !shouldNotExecute[statement.ObjectType]
	}
	executeStatements(statements, showProgressBar, objectsTitle, shouldExec)
}

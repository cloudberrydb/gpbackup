package restore

import "github.com/greenplum-db/gpbackup/utils"

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */

var (
	backupConfig  *utils.BackupConfig
	connection    *utils.DBConn
	globalCluster utils.Cluster
	globalTOC     *utils.TOC
	logger        *utils.Logger
	version       string
)

/*
 * Command-line flags
 */

var (
	backupDir      *string
	createdb       *bool
	debug          *bool
	numJobs        *int
	printVersion   *bool
	quiet          *bool
	redirect       *string
	restoreGlobals *bool
	timestamp      *string
	verbose        *bool
	withStats      *bool
)

/*
 * Setter functions
 */

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
}

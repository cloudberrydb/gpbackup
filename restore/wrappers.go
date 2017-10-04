package restore

import (
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and restoring metadata, so that the logic for each object type
 * can all be in one place and restore.go can serve as a high-level look at the
 * overall restore flow.
 */

/*
 * Setup and validation wrapper functions
 */

func SetLoggerVerbosity() {
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
}

func InitializeConnection(dbname string) {
	connection = utils.NewDBConn(dbname)
	connection.Connect()
	_, err := connection.Exec("SET application_name TO 'gprestore'")
	utils.CheckError(err)
	connection.SetDatabaseVersion()
	_, err = connection.Exec("SET search_path TO pg_catalog")
	utils.CheckError(err)
	_, err = connection.Exec("SET gp_enable_segment_copy_checking TO false")
	utils.CheckError(err)
}

func InitializeBackupConfig() {
	backupConfig = utils.ReadConfigFile(globalCluster.GetConfigFilePath())
	utils.InitializeCompressionParameters(backupConfig.Compressed)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connection.Version)
}

func GetGlobalStatements(objectTypes ...string) []utils.StatementWithType {
	globalFilename := globalCluster.GetGlobalFilePath()
	globalFile := utils.MustOpenFileForReading(globalFilename)
	var statements []utils.StatementWithType
	if len(objectTypes) > 0 {
		statements = globalTOC.GetSQLStatementForObjectTypes(globalTOC.GlobalEntries, globalFile, "SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA")
	} else {
		statements = globalTOC.GetAllSQLStatements(globalTOC.GlobalEntries, globalFile)
	}
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	return statements
}

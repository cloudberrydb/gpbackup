package restore

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
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
	if backupConfig.SingleDataFile && *numJobs != 1 {
		logger.Fatal(errors.Errorf("Cannot use --jobs flag when restoring backups with a single data file per segment."), "")
	}
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetRestoreMetadataStatements(filename string, objectTypes ...string) []utils.StatementWithType {
	metadataFile := utils.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	if len(objectTypes) > 0 {
		statements = globalTOC.GetSQLStatementForObjectTypes(filename, metadataFile, objectTypes...)
	} else {
		statements = globalTOC.GetAllSQLStatements(filename, metadataFile)
	}
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, showProgressBar bool) {
	if connection.Version.AtLeast("5") {
		ExecuteAllStatementsExcept(statements, objectsTitle, showProgressBar, "GPDB4 SESSION GUCS")
	} else {
		ExecuteAllStatements(statements, objectsTitle, showProgressBar)
	}
}

func setGUCsBeforeDataRestore() {
	objectTypes := []string{"SESSION GUCS"}
	if connection.Version.Before("5") {
		objectTypes = append(objectTypes, "GPDB4 SESSION GUCS")
	}
	gucStatements := GetRestoreMetadataStatements(globalCluster.GetPredataFilePath(), objectTypes...)
	ExecuteRestoreMetadataStatements(gucStatements, "", false)

	query := fmt.Sprintf("SET gp_enable_segment_copy_checking TO false;")
	_, err := connection.Exec(query)
	utils.CheckError(err)
}

func restoreSingleTableData(entry utils.MasterDataEntry, tableNum uint32, totalTables int) {
	name := utils.MakeFQN(entry.Schema, entry.Name)
	if logger.GetVerbosity() > utils.LOGINFO {
		// No progress bar at this log level, so we note table count here
		logger.Verbose("Reading data for table %s from file (table %d of %d)", name, tableNum, totalTables)
	} else {
		logger.Verbose("Reading data for table %s from file", name)
	}
	backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(entry.Oid, backupConfig.SingleDataFile)
	CopyTableIn(connection, name, entry.AttributeString, backupFile, backupConfig.SingleDataFile, tableNum-1)
}

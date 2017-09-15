package restore

import "github.com/greenplum-db/gpbackup/utils"

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
	_, err = connection.Exec("SET search_path TO pg_catalog")
	utils.CheckError(err)
}

func InitializeBackupReport() {
	reportFile := utils.MustOpenFileForReading(globalCluster.GetReportFilePath())
	backupReport = utils.ReadReportFile(reportFile)
	backupReport.SetBackupTypeFromString()
	utils.InitializeCompressionParameters(backupReport.Compressed)
	utils.EnsureBackupVersionCompatibility(backupReport.BackupVersion, version)
}

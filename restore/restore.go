package restore

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backupdir", "", "The absolute path of the directory in which the backup files to be restored are located")
	createdb = flag.Bool("createdb", false, "Create the database before metadata restore")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	redirect = flag.String("redirect", "", "Restore to the specified database instead of the database that was backed up")
	restoreGlobals = flag.Bool("globals", false, "Restore global metadata")
	timestamp = flag.String("timestamp", "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	withStats = flag.Bool("with-stats", false, "Restore query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gprestore", ""))
	initializeFlags()
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gprestore %s\n", version)
		os.Exit(0)
	}
	ValidateFlagCombinations()
	utils.ValidateBackupDir(*backupDir)
	if !utils.IsValidTimestamp(*timestamp) {
		logger.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", *timestamp), "")
	}
	logger.Info("Restore Key = %s", *timestamp)
}

func ValidateFlagCombinations() {
	utils.CheckMandatoryFlags("timestamp")
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	InitializeConnection("postgres")

	logger.Verbose("Gathering information on backup directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, *timestamp)
	globalCluster.VerifyBackupDirectoriesExistOnAllHosts()

	InitializeBackupConfig()
	globalCluster.VerifyMetadataFilePaths(backupConfig.DataOnly, *withStats, backupConfig.TableFiltered)
}

func DoRestore() {
	tocFilename := globalCluster.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeEntryMapFromCluster(globalCluster)
	if *restoreGlobals {
		restoreGlobal()
	} else if *createdb {
		createDatabase()
	}

	connection.Close()
	restoreDatabase := ""
	if *redirect != "" {
		restoreDatabase = *redirect
	} else {
		restoreDatabase = backupConfig.DatabaseName
	}
	InitializeConnection(restoreDatabase)

	if !backupConfig.DataOnly {
		restorePredata()
	}

	if !backupConfig.MetadataOnly {
		tableMap := GetTableDataEntriesFromTOC()
		backupFileCount := len(tableMap)
		globalCluster.VerifyBackupFileCountOnSegments(backupFileCount)
		restoreData(tableMap)
	}

	if !backupConfig.DataOnly && !backupConfig.TableFiltered {
		restorePostdata()
	}

	if *withStats && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase() {
	objectTypes := []string{"SESSION GUCS", "GPDB4 SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA"}
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Creating database")
	statements := GetRestoreMetadataStatements(globalFilename, objectTypes...)
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	ExecuteRestoreMetadataStatements(statements)
	logger.Info("Database creation complete")
}

func restoreGlobal() {
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Restoring global database metadata from %s", globalCluster.GetGlobalFilePath())
	statements := GetRestoreMetadataStatements(globalFilename)
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	ExecuteRestoreMetadataStatements(statements)
	logger.Info("Global database metadata restore complete")
}

func restorePredata() {
	predataFilename := globalCluster.GetPredataFilePath()
	logger.Info("Restoring pre-data metadata from %s", predataFilename)
	statements := GetRestoreMetadataStatements(predataFilename)
	ExecuteRestoreMetadataStatements(statements)
	logger.Info("Pre-data metadata restore complete")
}

func restoreData(tableMap map[uint32]utils.DataEntry) {
	numTables := 1
	logger.Info("Restoring data")
	dataProgressBar := utils.NewProgressBar(len(tableMap), "Tables restored: ")
	for oid, entry := range tableMap {
		name := utils.MakeFQN(entry.Schema, entry.Name)
		if logger.GetVerbosity() > utils.LOGINFO {
			// No progress bar at this log level, so we note table count here
			logger.Verbose("Reading data for table %s from file (table %d of %d)", name, numTables, len(tableMap))
		} else {
			logger.Verbose("Reading data for table %s from file", name)
		}
		backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(oid)
		CopyTableIn(connection, name, entry.AttributeString, backupFile)
		numTables++
		utils.IncrementProgressBar(dataProgressBar)
	}
	utils.FinishProgressBar(dataProgressBar)
	logger.Info("Data restore complete")
}

func restorePostdata() {
	postdataFilename := globalCluster.GetPostdataFilePath()
	logger.Info("Restoring post-data metadata from %s", postdataFilename)
	statements := GetRestoreMetadataStatements(postdataFilename)
	ExecuteRestoreMetadataStatements(statements)
	logger.Info("Post-data metadata restore complete")
}

func restoreStatistics() {
	statisticsFilename := globalCluster.GetStatisticsFilePath()
	logger.Info("Restoring query planner statistics from %s", statisticsFilename)
	statements := GetRestoreMetadataStatements(statisticsFilename)
	ExecuteRestoreMetadataStatements(statements)
	logger.Info("Query planner statistics restore complete")
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
		if connection != nil {
			if strings.Contains(errStr, fmt.Sprintf(`Database "%s" does not exist`, connection.DBName)) {
				errStr = fmt.Sprintf(`%s.  Use the --createdb flag to create "%s" as part of the restore process.`, errStr, connection.DBName)
			} else if strings.Contains(errStr, fmt.Sprintf(`Database "%s" already exists`, connection.DBName)) {
				errStr = fmt.Sprintf(`%s.  Run gprestore again without the --createdb flag.`, errStr)
			}
		}
		fmt.Println(errStr)
	}
	_, exitCode := utils.ParseErrorMessage(errStr)
	if connection != nil {
		connection.Close()
	}

	os.Exit(exitCode)
}

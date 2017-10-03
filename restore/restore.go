package restore

import (
	"flag"
	"fmt"
	"os"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

var (
	connection    *utils.DBConn
	logger        *utils.Logger
	globalCluster utils.Cluster
	globalTOC     *utils.TOC
	backupConfig  *utils.BackupConfig
	version       string
)

var ( // Command-line flags
	createdb       *bool
	debug          *bool
	backupDir      *string
	quiet          *bool
	timestamp      *string
	verbose        *bool
	restoreGlobals *bool
	printVersion   *bool
	withStats      *bool
)

// We define and initialize flags separately to avoid import conflicts in tests
func initializeFlags() {
	createdb = flag.Bool("createdb", false, "Create the database before metadata restore")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	backupDir = flag.String("backupdir", "", "The directory in which the backup files to be restored are located")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	timestamp = flag.String("timestamp", "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	restoreGlobals = flag.Bool("globals", false, "Restore global metadata")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	withStats = flag.Bool("with-stats", false, "Restore query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gprestore", ""))
	initializeFlags()
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
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
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckMandatoryFlags("timestamp")
	if !utils.IsValidTimestamp(*timestamp) {
		logger.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", *timestamp), "")
	}
	logger.Info("Restore Key = %s", *timestamp)
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
	globalCluster.VerifyMetadataFilePaths(backupConfig.DataOnly, *withStats)
}

func DoRestore() {
	tocFilename := globalCluster.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	if *restoreGlobals {
		restoreGlobal()
	} else if *createdb {
		createDatabase()
	}

	connection.Close()
	InitializeConnection(backupConfig.DatabaseName)

	if !backupConfig.DataOnly {
		restorePredata()
	}

	if !backupConfig.MetadataOnly {
		tableMap := GetTableDataEntriesFromTOC()
		backupFileCount := len(tableMap)
		globalCluster.VerifyBackupFileCountOnSegments(backupFileCount)
		restoreData(tableMap)
	}

	if !backupConfig.DataOnly {
		restorePostdata()
	}

	if *withStats && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase() {
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Creating database")
	globalFile := utils.MustOpenFileForReaderAt(globalFilename)
	statements := globalTOC.GetSQLStatementForObjectTypes(globalTOC.GlobalEntries, globalFile, "SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA")
	for _, statement := range statements {
		_, err := connection.Exec(statement)
		utils.CheckError(err)
	}
	logger.Info("Database creation complete")
}

func restoreGlobal() {
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Restoring global database metadata from %s", globalFilename)
	utils.ExecuteSQLFile(connection, globalFilename)
	logger.Info("Global database metadata restore complete")
}

func restorePredata() {
	predataFilename := globalCluster.GetPredataFilePath()
	logger.Info("Restoring pre-data metadata from %s", predataFilename)
	utils.ExecuteSQLFile(connection, predataFilename)
	logger.Info("Pre-data metadata restore complete")
}

func restoreData(tableMap map[uint32]utils.DataEntry) {
	logger.Info("Restoring data")
	for oid, entry := range tableMap {
		name := utils.MakeFQN(entry.Schema, entry.Name)
		logger.Verbose("Reading data for table %s from file", name)
		backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(oid)
		CopyTableIn(connection, name, entry.AttributeString, backupFile)
	}
	logger.Info("Data restore complete")
}

func restorePostdata() {
	postdataFilename := globalCluster.GetPostdataFilePath()
	logger.Info("Restoring post-data metadata from %s", postdataFilename)
	utils.ExecuteSQLFile(connection, postdataFilename)
	logger.Info("Post-data metadata restore complete")
}

func restoreStatistics() {
	statisticsFilename := globalCluster.GetStatisticsFilePath()
	logger.Info("Restoring query planner statistics from %s", statisticsFilename)
	utils.ExecuteSQLFile(connection, statisticsFilename)
	logger.Info("Query planner statistics restore complete")
}

func DoTeardown() {
	var err interface{}
	if err = recover(); err != nil {
		fmt.Println(err)
	}
	_, exitCode := utils.ParseErrorMessage(err)
	if connection != nil {
		connection.Close()
	}

	os.Exit(exitCode)
}

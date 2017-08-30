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
	backupReport  utils.Report
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
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gprestore", ""))
	initializeFlags()
}

func SetLogger(log *utils.Logger) {
	logger = log
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
	InitializeConnection("postgres)")
	InitializeBackupReport()

	logger.Verbose("Gathering information on backup directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, *timestamp)
	globalCluster.VerifyBackupDirectoriesExistOnAllHosts()
}

func DoRestore() {
	if *restoreGlobals {
		restoreGlobal()
	} else if *createdb {
		createDatabase()
	}

	connection.Close()
	InitializeConnection(backupReport.DatabaseName)

	if !backupReport.DataOnly {
		restorePredata()
	}

	if !backupReport.MetadataOnly {
		tableMap := ReadTableMapFile(globalCluster.GetTableMapFilePath())
		backupFileCount := len(tableMap)
		globalCluster.VerifyBackupFileCountOnSegments(backupFileCount)
		restoreData(tableMap)
	}

	if !backupReport.DataOnly {
		restorePostdata()
	}
}

func createDatabase() {
	globalFilename := utils.GetGlobalFilename(globalCluster)
	tocFilename := utils.GetTOCFilename(globalCluster)
	logger.Info("Creating database")
	toc := utils.NewTOC(tocFilename)
	globalFile := utils.MustOpenFileForReaderAt(globalFilename)
	statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA")
	for _, statement := range statements {
		_, err := connection.Exec(statement)
		utils.CheckError(err)
	}
	logger.Info("Database creation complete")
}

func restoreGlobal() {
	globalFilename := utils.GetGlobalFilename(globalCluster)
	logger.Info("Restoring global database metadata from %s", globalFilename)
	utils.ExecuteSQLFile(connection, globalFilename)
	logger.Info("Global database metadata restore complete")
}

func restorePredata() {
	predataFilename := utils.GetPredataFilename(globalCluster)
	logger.Info("Restoring pre-data metadata from %s", predataFilename)
	utils.ExecuteSQLFile(connection, predataFilename)
	logger.Info("Pre-data metadata restore complete")
}

func restoreData(tableMap map[string]uint32) {
	logger.Info("Restoring data")
	for name, oid := range tableMap {
		logger.Verbose("Reading data for table %s from file", name)
		backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(oid)
		CopyTableIn(connection, name, backupFile)
	}
	logger.Info("Data restore complete")
}

func restorePostdata() {
	postdataFilename := utils.GetPostdataFilename(globalCluster)
	logger.Info("Restoring post-data metadata from %s", postdataFilename)
	utils.ExecuteSQLFile(connection, postdataFilename)
	logger.Info("Post-data metadata restore complete")
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

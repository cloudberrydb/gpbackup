package backup

import (
	"flag"
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v2"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	connection    *utils.DBConn
	logger        *utils.Logger
	globalCluster utils.Cluster
	globalTOC     *utils.TOC
	objectCounts  map[string]int
	backupReport  *utils.Report
	version       string
)

var ( // Command-line flags
	dataOnly       *bool
	dbname         *string
	metadataOnly   *bool
	debug          *bool
	backupDir      *string
	backupGlobals  *bool
	noCompress     *bool
	printVersion   *bool
	quiet          *bool
	verbose        *bool
	includeSchemas utils.ArrayFlags
)

// We define and initialize flags separately to avoid import conflicts in tests
func initializeFlags() {
	dataOnly = flag.Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = flag.String("dbname", "", "The database to be backed up")
	metadataOnly = flag.Bool("metadata-only", false, "Only back up metadata, do not back up data")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	backupDir = flag.String("backupdir", "", "The directory to which all backup files will be written")
	backupGlobals = flag.Bool("globals", false, "Back up global metadata")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	flag.Var(&includeSchemas, "include-schema", "Back up only the specified schema(s). --include-schema can be specified multiple times.")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gpbackup", ""))
	initializeFlags()
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetCluster(cluster utils.Cluster) {
	globalCluster = cluster
}

func SetVersion(v string) {
	version = v
}

func SetIncludeSchemas(schemas []string) {
	includeSchemas = schemas
}

func DoFlagValidation() {
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup %s\n", version)
		os.Exit(0)
	}
	ValidateFlagCombinations()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	InitializeConnection()
	InitializeBackupReport()

	validateSetup()

	segConfig := utils.GetSegmentConfiguration(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, utils.CurrentTimestamp())
	globalCluster.CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
}

/*
 * This function handles validation that must be done after parsing flags.
 * It should only validate, and either error out or complete without side effects;
 * initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func validateSetup() {
	ValidateIncludeSchemas(connection)
}

func DoBackup() {
	LogBackupInfo()

	objectCounts = make(map[string]int, 0)
	tables, tableDefs := RetrieveAndLockTables(objectCounts)

	if !*dataOnly {
		backupGlobal(objectCounts)
		backupPredata(tables, tableDefs, objectCounts)
		backupPostdata(objectCounts)
		writeTOC(utils.GetTOCFilename(globalCluster), globalTOC)
	}

	if !*metadataOnly {
		backupData(tables, tableDefs)
	}

	connection.Commit()
}

func writeTOC(filename string, toc *utils.TOC) {
	tocFile := utils.MustOpenFileForWriting(filename)
	tocContents, _ := yaml.Marshal(toc)
	utils.MustPrintBytes(tocFile, tocContents)
}

func backupGlobal(objectCounts map[string]int) {
	globalFilename := utils.GetGlobalFilename(globalCluster)
	logger.Info("Writing global database metadata to %s", globalFilename)
	globalFile := utils.NewFileWithByteCountFromFile(globalFilename)
	defer globalFile.Close()

	BackupGlobalSessionGUCs(globalFile, objectCounts)
	BackupTablespaces(globalFile, objectCounts)
	BackupCreateDatabase(globalFile, objectCounts)
	BackupDatabaseGUCs(globalFile, objectCounts)

	if len(includeSchemas) == 0 {
		BackupResourceQueues(globalFile, objectCounts)
		BackupRoles(globalFile, objectCounts)
		BackupRoleGrants(globalFile, objectCounts)
	}
	logger.Info("Global database metadata backup complete")
}

func backupPredata(tables []Relation, tableDefs map[uint32]TableDefinition, objectCounts map[string]int) {
	predataFilename := utils.GetPredataFilename(globalCluster)
	logger.Info("Writing pre-data metadata to %s", predataFilename)
	predataFile := utils.NewFileWithByteCountFromFile(predataFilename)
	defer predataFile.Close()

	PrintConnectionString(predataFile, connection.DBName)

	BackupPredataSessionGUCs(predataFile, objectCounts)
	BackupSchemas(predataFile, objectCounts)

	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs, types, functionMetadata, typeMetadata, funcInfoMap := RetrieveFunctionsAndTypes(objectCounts, procLangs)

	if len(includeSchemas) == 0 {
		BackupProceduralLanguages(predataFile, objectCounts, procLangs, langFuncs, functionMetadata, funcInfoMap)
	}

	BackupShellTypes(predataFile, objectCounts, types)
	BackupEnumTypes(predataFile, objectCounts, types, typeMetadata)

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)
	sequences := GetAllSequences(connection)
	BackupCreateSequences(predataFile, objectCounts, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints(objectCounts)

	BackupFunctionsAndTypesAndTables(predataFile, otherFuncs, types, tables, functionMetadata, typeMetadata, relationMetadata, tableDefs, constraints)
	BackupAlterSequences(predataFile, objectCounts, sequences)

	if len(includeSchemas) == 0 {
		BackupProtocols(predataFile, objectCounts, funcInfoMap)
	}

	BackupTSParsers(predataFile, objectCounts)
	BackupTSTemplates(predataFile, objectCounts)
	BackupTSDictionaries(predataFile, objectCounts)
	BackupTSConfigurations(predataFile, objectCounts)

	BackupOperators(predataFile, objectCounts)
	BackupOperatorFamilies(predataFile, objectCounts)
	BackupOperatorClasses(predataFile, objectCounts)

	BackupConversions(predataFile, objectCounts)
	BackupAggregates(predataFile, objectCounts, funcInfoMap)
	BackupCasts(predataFile, objectCounts)
	BackupViews(predataFile, objectCounts, relationMetadata)
	BackupConstraints(predataFile, objectCounts, constraints, conMetadata)
	logger.Info("Pre-data metadata backup complete")
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Info("Writing data to file")
	BackupData(tables, tableDefs)
	WriteTableMapFile(globalCluster.GetTableMapFilePath(), tables, tableDefs)
	logger.Info("Data backup complete")
}

func backupPostdata(objectCounts map[string]int) {
	postdataFilename := utils.GetPostdataFilename(globalCluster)
	logger.Info("Writing post-data metadata to %s", postdataFilename)
	postdataFile := utils.NewFileWithByteCountFromFile(postdataFilename)
	defer postdataFile.Close()

	PrintConnectionString(postdataFile, connection.DBName)

	BackupPostdataSessionGUCs(postdataFile, objectCounts)
	BackupIndexes(postdataFile, objectCounts)
	BackupRules(postdataFile, objectCounts)
	BackupTriggers(postdataFile, objectCounts)
	logger.Info("Post-data metadata backup complete")
}

func DoTeardown() {
	var err interface{}
	if err = recover(); err != nil {
		fmt.Println(err)
	}
	errMsg, exitCode := utils.ParseErrorMessage(err)
	if connection != nil {
		connection.Close()
	}

	// Only create a report file if we fail after the cluster is initialized
	if globalCluster.Timestamp != "" {
		reportFilename := globalCluster.GetReportFilePath()
		reportFile := utils.MustOpenFileForWriting(reportFilename)
		utils.WriteReportFile(reportFile, globalCluster.Timestamp, backupReport, objectCounts, errMsg)
	}

	os.Exit(exitCode)
}

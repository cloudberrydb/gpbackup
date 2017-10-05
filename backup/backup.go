package backup

import (
	"flag"
	"fmt"
	"os"
	"time"

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
	backupDir        *string
	backupGlobals    *bool
	dataOnly         *bool
	dbname           *string
	debug            *bool
	excludeSchemas   utils.ArrayFlags
	excludeTableFile *string
	excludeTables    utils.ArrayFlags
	includeSchemas   utils.ArrayFlags
	includeTableFile *string
	includeTables    utils.ArrayFlags
	metadataOnly     *bool
	noCompression    *bool
	printVersion     *bool
	quiet            *bool
	verbose          *bool
	withStats        *bool
)

// We define and initialize flags separately to avoid import conflicts in tests
func initializeFlags() {
	backupDir = flag.String("backupdir", "", "The directory to which all backup files will be written")
	backupGlobals = flag.Bool("globals", false, "Back up global metadata")
	dataOnly = flag.Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = flag.String("dbname", "", "The database to be backed up")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&excludeSchemas, "exclude-schema", "Do not back up only the specified schema(s). --exclude-schema can be specified multiple times.")
	excludeTableFile = flag.String("exclude-table-file", "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flag.Var(&includeSchemas, "include-schema", "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	includeTableFile = flag.String("include-table-file", "", "A file containing a list of fully-qualified tables to be included in the backup")
	metadataOnly = flag.Bool("metadata-only", false, "Only back up metadata, do not back up data")
	noCompression = flag.Bool("no-compression", false, "Disable compression of data files")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	withStats = flag.Bool("with-stats", false, "Back up query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gpbackup", ""))
	initializeFlags()
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetConnection(conn *utils.DBConn) {
	connection = conn
}

func SetCluster(cluster utils.Cluster) {
	globalCluster = cluster
}

func SetVersion(v string) {
	version = v
}

func SetExcludeSchemas(schemas []string) {
	excludeSchemas = schemas
}

func SetIncludeSchemas(schemas []string) {
	includeSchemas = schemas
}

func SetExcludeTables(tables []string) {
	excludeTables = tables
}

func SetIncludeTables(tables []string) {
	includeTables = tables
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
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

	InitializeFilterLists()
	validateSetup()

	segConfig := utils.GetSegmentConfiguration(connection)
	timestamp := utils.CurrentTimestamp()
	utils.CreateBackupLockFile(timestamp)
	globalCluster = utils.NewCluster(segConfig, *backupDir, timestamp)
	globalCluster.CreateBackupDirectoriesOnAllHosts()
	InitializeTOC()
}

/*
 * This function handles validation that must be done after parsing flags.
 * It should only validate, and either error out or complete without side effects;
 * initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func validateSetup() {
	ValidateFilterSchemas(connection, excludeSchemas)
	ValidateFilterSchemas(connection, includeSchemas)
	ValidateFilterTables(connection, excludeTables)
	ValidateFilterTables(connection, includeTables)
}

func DoBackup() {
	LogBackupInfo()

	objectCounts = make(map[string]int, 0)
	tables, tableDefs := RetrieveAndLockTables(objectCounts)

	isTableFiltered := len(includeTables) > 0 || len(excludeTables) > 0

	if !*dataOnly {
		if isTableFiltered {
			backupTablePredata(tables, tableDefs, objectCounts)
		} else {
			backupGlobal(objectCounts)
			backupPredata(tables, tableDefs, objectCounts)
			backupPostdata(objectCounts)
		}
	}

	if !*metadataOnly {
		backupData(tables, tableDefs)
	}

	if *withStats {
		backupStatistics(tables)
	}

	globalTOC.WriteToFile(globalCluster.GetTOCFilePath())
	connection.Commit()
}

func backupGlobal(objectCounts map[string]int) {
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Writing global database metadata to %s", globalFilename)
	globalFile := utils.NewFileWithByteCountFromFile(globalFilename)
	defer globalFile.Close()

	BackupSessionGUCs(globalFile)
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
	predataFilename := globalCluster.GetPredataFilePath()
	logger.Info("Writing pre-data metadata to %s", predataFilename)
	predataFile := utils.NewFileWithByteCountFromFile(predataFilename)
	defer predataFile.Close()

	BackupSessionGUCs(predataFile)
	BackupSchemas(predataFile, objectCounts)

	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs, types, functionMetadata, typeMetadata, funcInfoMap := RetrieveFunctionsAndTypes(objectCounts, procLangs)

	if len(includeSchemas) == 0 {
		BackupProceduralLanguages(predataFile, objectCounts, procLangs, langFuncs, functionMetadata, funcInfoMap)
	}

	BackupShellTypes(predataFile, objectCounts, types)
	if connection.Version.AtLeast("5") {
		BackupEnumTypes(predataFile, objectCounts, types, typeMetadata)
	}

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)
	sequences := GetAllSequences(connection)
	BackupCreateSequences(predataFile, objectCounts, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints(objectCounts)

	BackupFunctionsAndTypesAndTables(predataFile, otherFuncs, types, tables, functionMetadata, typeMetadata, relationMetadata, tableDefs, constraints)
	BackupAlterSequences(predataFile, objectCounts, sequences)

	if len(includeSchemas) == 0 {
		BackupProtocols(predataFile, objectCounts, funcInfoMap)
	}

	if connection.Version.AtLeast("5") {
		BackupTSParsers(predataFile, objectCounts)
		BackupTSTemplates(predataFile, objectCounts)
		BackupTSDictionaries(predataFile, objectCounts)
		BackupTSConfigurations(predataFile, objectCounts)
	}

	BackupOperators(predataFile, objectCounts)
	if connection.Version.AtLeast("5") {
		BackupOperatorFamilies(predataFile, objectCounts)
	}
	BackupOperatorClasses(predataFile, objectCounts)

	BackupConversions(predataFile, objectCounts)
	BackupAggregates(predataFile, objectCounts, funcInfoMap)
	BackupCasts(predataFile, objectCounts)
	BackupViews(predataFile, objectCounts, relationMetadata)
	BackupConstraints(predataFile, objectCounts, constraints, conMetadata)
	logger.Info("Pre-data metadata backup complete")
}

func backupTablePredata(tables []Relation, tableDefs map[uint32]TableDefinition, objectCounts map[string]int) {
	predataFilename := globalCluster.GetPredataFilePath()
	logger.Info("Writing table metadata to %s", predataFilename)
	predataFile := utils.NewFileWithByteCountFromFile(predataFilename)
	defer predataFile.Close()

	BackupSessionGUCs(predataFile)

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)

	constraints, conMetadata := RetrieveConstraints(objectCounts, tables...)

	BackupTables(predataFile, tables, relationMetadata, tableDefs, constraints)
	BackupConstraints(predataFile, objectCounts, constraints, conMetadata)
	logger.Info("Table metadata backup complete")
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Info("Writing data to file")
	BackupData(tables, tableDefs)
	AddTableDataEntriesToTOC(tables, tableDefs)
	logger.Info("Data backup complete")
}

func backupPostdata(objectCounts map[string]int) {
	postdataFilename := globalCluster.GetPostdataFilePath()
	logger.Info("Writing post-data metadata to %s", postdataFilename)
	postdataFile := utils.NewFileWithByteCountFromFile(postdataFilename)
	defer postdataFile.Close()

	BackupSessionGUCs(postdataFile)
	BackupIndexes(postdataFile, objectCounts)
	BackupRules(postdataFile, objectCounts)
	BackupTriggers(postdataFile, objectCounts)
	logger.Info("Post-data metadata backup complete")
}

func backupStatistics(tables []Relation) {
	statisticsFilename := globalCluster.GetStatisticsFilePath()
	logger.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	BackupStatistics(statisticsFile, tables)
	logger.Info("Query planner statistics backup complete")
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
		fmt.Println(err)
	}
	errMsg, exitCode := utils.ParseErrorMessage(errStr)
	if connection != nil {
		connection.Close()
	}

	// Only create a report file if we fail after the cluster is initialized
	if globalCluster.Timestamp != "" {
		reportFilename := globalCluster.GetReportFilePath()
		configFilename := globalCluster.GetConfigFilePath()
		backupReport.WriteReportFile(reportFilename, globalCluster.Timestamp, objectCounts, errMsg)
		backupReport.WriteConfigFile(configFilename)
		utils.EmailReport(globalCluster)
		// We sleep for 1 second to ensure multiple backups do not start within the same second.
		time.Sleep(1000 * time.Millisecond)
		timestampLockFile := fmt.Sprintf("/tmp/%s.lck", globalCluster.Timestamp)
		err := os.Remove(timestampLockFile)
		if err != nil {
			logger.Warn("Failed to remove lock file %s.", timestampLockFile)
		}
	}

	os.Exit(exitCode)
}

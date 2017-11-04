package backup

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backupdir", "", "The absolute path of the directory to which all backup files will be written")
	dataOnly = flag.Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = flag.String("dbname", "", "The database to be backed up")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&excludeSchemas, "exclude-schema", "Do not back up only the specified schema(s). --exclude-schema can be specified multiple times.")
	excludeTableFile = flag.String("exclude-table-file", "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flag.Var(&includeSchemas, "include-schema", "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	includeTableFile = flag.String("include-table-file", "", "A file containing a list of fully-qualified tables to be included in the backup")
	leafPartitionData = flag.Bool("leaf-partition-data", false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
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
	utils.ValidateBackupDir(*backupDir)
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	logger.Info("Starting backup of database %s", *dbname)
	InitializeConnection()

	InitializeFilterLists()
	InitializeBackupReport()
	validateSetup()

	segConfig := utils.GetSegmentConfiguration(connection)
	timestamp := utils.CurrentTimestamp()
	segPrefix := utils.GetSegPrefix(connection)
	utils.CreateBackupLockFile(timestamp)
	globalCluster = utils.NewCluster(segConfig, *backupDir, timestamp, segPrefix)
	globalCluster.CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeEntryMapFromCluster(globalCluster)
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

	metadataTables, dataTables := SplitTablesByPartitionType(tables, tableDefs)
	if !*dataOnly {
		if isTableFiltered {
			backupTablePredata(metadataTables, tableDefs, objectCounts)
		} else {
			backupGlobal(objectCounts)
			backupPredata(metadataTables, tableDefs, objectCounts)
			backupPostdata(objectCounts)
		}
	}

	if !*metadataOnly {
		backupData(dataTables, tableDefs)
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
		if connection.Version.AtLeast("5") {
			BackupResourceGroups(globalFile, objectCounts)
		}
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
	langFuncs, otherFuncs, functionMetadata := RetrieveFunctions(objectCounts, procLangs)
	types, typeMetadata, funcInfoMap := RetrieveTypes(objectCounts)

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

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalCluster.Timestamp != "" {
		_, statErr := os.Stat(globalCluster.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			os.Exit(exitCode)
		}
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

	if exitCode == 0 {
		logger.Info("Backup completed successfully")
	}
	os.Exit(exitCode)
}

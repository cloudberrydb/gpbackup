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
	dataOnly      *bool
	dbname        *string
	metadataOnly  *bool
	debug         *bool
	backupDir     *string
	backupGlobals *bool
	noCompress    *bool
	printVersion  *bool
	quiet         *bool
	verbose       *bool
	schemaInclude utils.ArrayFlags
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
	flag.Var(&schemaInclude, "schema-include", "Back up only specified schema(s). --schema-include can be specified multiple times.")
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

func SetSchemaInclude(schemas []string) {
	schemaInclude = schemas
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
		fmt.Printf("gpbackup %s\n", version)
		os.Exit(0)
	}
	utils.CheckMandatoryFlags("dbname")
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckExclusiveFlags("data-only", "metadata-only")
}

func ValidateWithConnection(connection *utils.DBConn) {
	if len(schemaInclude) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(schemaInclude)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := SelectStringSlice(connection, query)
		if len(resultSchemas) < len(schemaInclude) {
			schemaMap := make(map[string]bool)
			for _, schema := range resultSchemas {
				schemaMap[schema] = true
			}

			for _, schema := range schemaInclude {
				if _, ok := schemaMap[schema]; !ok {
					logger.Fatal(nil, "Schema %s does not exist", schema)
				}
			}
		}
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
	connection = utils.NewDBConn(*dbname)
	connection.Connect()
	connection.Exec("SET application_name TO 'gpbackup'")
	ValidateWithConnection(connection)

	backupReport = &utils.Report{
		DatabaseName:    connection.DBName,
		DatabaseVersion: connection.GetDatabaseVersion(),
		BackupVersion:   version,
		DatabaseSize:    connection.GetDBSize(),
	}
	backupReport.SetBackupTypeFromFlags(*dataOnly, *metadataOnly, schemaInclude)

	logger.Verbose("Creating backup directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, utils.CurrentTimestamp())
	globalCluster.CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
}

func DoBackup() {
	logger.Info("Backup Timestamp = %s", globalCluster.Timestamp)
	logger.Info("Backup Database = %s", utils.QuoteIdent(connection.DBName))
	logger.Info("Backup Type = %s", backupReport.BackupType)

	masterBackupDir := globalCluster.GetDirForContent(-1)
	objectCounts = make(map[string]int, 0)

	globalFilename := fmt.Sprintf("%s/global.sql", masterBackupDir)
	predataFilename := fmt.Sprintf("%s/predata.sql", masterBackupDir)
	postdataFilename := fmt.Sprintf("%s/postdata.sql", masterBackupDir)
	tocFilename := fmt.Sprintf("%s/toc.yaml", masterBackupDir)

	connection.Begin()
	connection.Exec("SET search_path TO pg_catalog")

	tables := GetAllUserTables(connection)
	logger.Info("Acquiring ACCESS SHARE locks on tables")
	LockTables(connection, tables)
	logger.Info("Locks acquired")
	objectCounts["Tables"] = len(tables)
	tableDefs := ConstructDefinitionsForTables(connection, tables)

	if !*dataOnly {
		logger.Info("Writing global database metadata to %s", globalFilename)
		backupGlobal(globalFilename, objectCounts)
		logger.Info("Global database metadata backup complete")

		logger.Info("Writing pre-data metadata to %s", predataFilename)
		backupPredata(predataFilename, tables, tableDefs, objectCounts)
		logger.Info("Pre-data metadata backup complete")

		logger.Info("Writing post-data metadata to %s", postdataFilename)
		backupPostdata(postdataFilename, objectCounts)
		logger.Info("Post-data metadata backup complete")
		writeTOC(tocFilename, globalTOC)
	}

	if !*metadataOnly {
		logger.Info("Writing data to file")
		backupData(tables, tableDefs)
		logger.Info("Data backup complete")
	}

	connection.Commit()
}

func writeTOC(filename string, toc *utils.TOC) {
	tocFile := utils.MustOpenFileForWriting(filename)
	tocContents, _ := yaml.Marshal(toc)
	utils.MustPrintBytes(tocFile, tocContents)
}

func backupGlobal(filename string, objectCount map[string]int) {
	globalFile := utils.NewFileWithByteCountFromFile(filename)

	logger.Verbose("Writing session GUCs to global file")
	gucs := GetSessionGUCs(connection)
	PrintGlobalSessionGUCs(globalFile, globalTOC, gucs)

	logger.Verbose("Writing CREATE TABLESPACE statements to global file")
	tablespaces := GetTablespaces(connection)
	objectCount["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(connection, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(globalFile, globalTOC, tablespaces, tablespaceMetadata)

	logger.Verbose("Writing CREATE DATABASE statement to global file")
	dbnames := GetDatabaseNames(connection)
	dbMetadata := GetMetadataForObjectType(connection, TYPE_DATABASE)
	PrintCreateDatabaseStatement(globalFile, globalTOC, connection.DBName, dbnames, dbMetadata, *backupGlobals)

	logger.Verbose("Writing database GUCs to global file")
	databaseGucs := GetDatabaseGUCs(connection)
	objectCount["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(globalFile, globalTOC, databaseGucs, connection.DBName)

	if len(schemaInclude) == 0 {
		logger.Verbose("Writing CREATE RESOURCE QUEUE statements to global file")
		resQueues := GetResourceQueues(connection)
		objectCount["Resource Queues"] = len(resQueues)
		resQueueMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEQUEUE)
		PrintCreateResourceQueueStatements(globalFile, globalTOC, resQueues, resQueueMetadata)

		logger.Verbose("Writing CREATE ROLE statements to global file")
		roles := GetRoles(connection)
		objectCount["Roles"] = len(roles)
		roleMetadata := GetCommentsForObjectType(connection, TYPE_ROLE)
		PrintCreateRoleStatements(globalFile, globalTOC, roles, roleMetadata)

		logger.Verbose("Writing GRANT ROLE statements to global file")
		roleMembers := GetRoleMembers(connection)
		PrintRoleMembershipStatements(globalFile, globalTOC, roleMembers)
	}

	globalFile.Close()
}

func backupPredata(filename string, tables []Relation, tableDefs map[uint32]TableDefinition, objectCount map[string]int) {
	predataFile := utils.NewFileWithByteCountFromFile(filename)

	PrintConnectionString(predataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintPredataSessionGUCs(predataFile, globalTOC, gucs)

	logger.Verbose("Writing CREATE SCHEMA statements to predata file")
	schemas := GetAllUserSchemas(connection)
	objectCount["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(connection, TYPE_SCHEMA)
	PrintCreateSchemaStatements(predataFile, globalTOC, schemas, schemaMetadata)

	types := GetTypes(connection)
	objectCount["Types"] = len(types)
	typeMetadata := GetMetadataForObjectType(connection, TYPE_TYPE)
	functions := GetFunctions(connection)
	objectCount["Functions"] = len(functions)
	funcInfoMap := GetFunctionOidToInfoMap(connection)
	functionMetadata := GetMetadataForObjectType(connection, TYPE_FUNCTION)
	functions, types = ConstructFunctionAndTypeDependencyLists(connection, functions, types)

	logger.Verbose("Writing CREATE TYPE statements for shell types to predata file")
	PrintCreateShellTypeStatements(predataFile, globalTOC, types)

	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)
	if len(schemaInclude) == 0 {
		logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to predata file")
		objectCount["Procedural Languages"] = len(procLangs)
		for _, langFunc := range langFuncs {
			PrintCreateFunctionStatement(predataFile, globalTOC, langFunc, functionMetadata[langFunc.Oid])
		}
		procLangMetadata := GetMetadataForObjectType(connection, TYPE_PROCLANGUAGE)
		PrintCreateLanguageStatements(predataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
	}

	logger.Verbose("Writing CREATE TYPE statements for enum types to predata file")
	PrintCreateEnumTypeStatements(predataFile, globalTOC, types, typeMetadata)

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)
	logger.Verbose("Writing CREATE SEQUENCE statements to predata file")
	sequences := GetAllSequences(connection)
	objectCount["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(predataFile, globalTOC, sequences, relationMetadata)

	tables = ConstructTableDependencies(connection, tables)

	constraints := GetConstraints(connection)
	conMetadata := GetCommentsForObjectType(connection, TYPE_CONSTRAINT)

	logger.Verbose("Writing CREATE TABLE statements to predata file")
	logger.Verbose("Writing CREATE FUNCTION statements and CREATE TYPE statements for base, composite, and domain types to predata file")
	sortedSlice := SortFunctionsAndTypesAndTablesInDependencyOrder(otherFuncs, types, tables)
	filteredMetadata := ConstructFunctionAndTypeAndTableMetadataMap(functionMetadata, typeMetadata, relationMetadata)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(predataFile, globalTOC, sortedSlice, filteredMetadata, tableDefs, constraints)

	logger.Verbose("Writing ALTER SEQUENCE statements to predata file")
	sequenceColumnOwners := GetSequenceColumnOwnerMap(connection)
	PrintAlterSequenceStatements(predataFile, globalTOC, sequences, sequenceColumnOwners)

	logger.Verbose("Writing CREATE TEXT SEARCH PARSER statements to predata file")
	parsers := GetTextSearchParsers(connection)
	objectCount["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(connection, TYPE_TSPARSER)
	PrintCreateTextSearchParserStatements(predataFile, globalTOC, parsers, parserMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH TEMPLATE statements to predata file")
	templates := GetTextSearchTemplates(connection)
	objectCount["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(connection, TYPE_TSTEMPLATE)
	PrintCreateTextSearchTemplateStatements(predataFile, globalTOC, templates, templateMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH DICTIONARY statements to predata file")
	dictionaries := GetTextSearchDictionaries(connection)
	objectCount["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(connection, TYPE_TSDICTIONARY)
	PrintCreateTextSearchDictionaryStatements(predataFile, globalTOC, dictionaries, dictionaryMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH CONFIGURATION statements to predata file")
	configurations := GetTextSearchConfigurations(connection)
	objectCount["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(connection, TYPE_TSCONFIGURATION)
	PrintCreateTextSearchConfigurationStatements(predataFile, globalTOC, configurations, configurationMetadata)

	if len(schemaInclude) == 0 {
		logger.Verbose("Writing CREATE PROTOCOL statements to predata file")
		protocols := GetExternalProtocols(connection)
		objectCount["Protocols"] = len(protocols)
		protoMetadata := GetMetadataForObjectType(connection, TYPE_PROTOCOL)
		PrintCreateExternalProtocolStatements(predataFile, globalTOC, protocols, funcInfoMap, protoMetadata)
	}

	logger.Verbose("Writing CREATE CONVERSION statements to predata file")
	conversions := GetConversions(connection)
	objectCount["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(connection, TYPE_CONVERSION)
	PrintCreateConversionStatements(predataFile, globalTOC, conversions, convMetadata)

	logger.Verbose("Writing CREATE OPERATOR statements to predata file")
	operators := GetOperators(connection)
	objectCount["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(connection, TYPE_OPERATOR)
	PrintCreateOperatorStatements(predataFile, globalTOC, operators, operatorMetadata)

	logger.Verbose("Writing CREATE OPERATOR FAMILY statements to predata file")
	operatorFamilies := GetOperatorFamilies(connection)
	objectCount["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(predataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)

	logger.Verbose("Writing CREATE OPERATOR CLASS statements to predata file")
	operatorClasses := GetOperatorClasses(connection)
	objectCount["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORCLASS)
	PrintCreateOperatorClassStatements(predataFile, globalTOC, operatorClasses, operatorClassMetadata)

	logger.Verbose("Writing CREATE AGGREGATE statements to predata file")
	aggregates := GetAggregates(connection)
	objectCount["Aggregates"] = len(aggregates)
	aggMetadata := GetMetadataForObjectType(connection, TYPE_AGGREGATE)
	PrintCreateAggregateStatements(predataFile, globalTOC, aggregates, funcInfoMap, aggMetadata)

	logger.Verbose("Writing CREATE CAST statements to predata file")
	casts := GetCasts(connection)
	objectCount["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(connection, TYPE_CAST)
	PrintCreateCastStatements(predataFile, globalTOC, casts, castMetadata)

	logger.Verbose("Writing CREATE VIEW statements to predata file")
	views := GetViews(connection)
	objectCount["Views"] = len(views)
	views = ConstructViewDependencies(connection, views)
	views = SortViews(views)
	PrintCreateViewStatements(predataFile, globalTOC, views, relationMetadata)

	logger.Verbose("Writing ADD CONSTRAINT statements to predata file")
	PrintConstraintStatements(predataFile, globalTOC, constraints, conMetadata)

	predataFile.Close()
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	numExtTables := 0
	for _, table := range tables {
		if !tableDefs[table.RelationOid].IsExternal {
			logger.Verbose("Writing data for table %s to file", table.ToString())
			backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(table.RelationOid)
			CopyTableOut(connection, table, backupFile)
		} else {
			logger.Verbose("Skipping data backup of table %s because it is an external table.", table.ToString())
			numExtTables++
		}
	}
	if numExtTables > 0 {
		s := ""
		if numExtTables > 1 {
			s = "s"
		}
		logger.Warn("Skipped data backup of %d external table%s.", numExtTables, s)
		logger.Warn("See %s for a complete list of skipped tables.", logger.GetLogFileName())
	}
	logger.Verbose("Writing table map file to %s", globalCluster.GetTableMapFilePath())
	WriteTableMapFile(globalCluster.GetTableMapFilePath(), tables, tableDefs)
}

func backupPostdata(filename string, objectCount map[string]int) {
	postdataFile := utils.NewFileWithByteCountFromFile(filename)

	PrintConnectionString(postdataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to postdata file")
	gucs := GetSessionGUCs(connection)
	PrintPostdataSessionGUCs(postdataFile, globalTOC, gucs)

	logger.Verbose("Writing CREATE INDEX statements to postdata file")
	indexNameMap := ConstructImplicitIndexNames(connection)
	indexes := GetIndexes(connection, indexNameMap)
	objectCount["Indexes"] = len(indexes)
	indexMetadata := GetCommentsForObjectType(connection, TYPE_INDEX)
	PrintCreateIndexStatements(postdataFile, globalTOC, indexes, indexMetadata)

	logger.Verbose("Writing CREATE RULE statements to postdata file")
	rules := GetRules(connection)
	objectCount["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(connection, TYPE_RULE)
	PrintCreateRuleStatements(postdataFile, globalTOC, rules, ruleMetadata)

	logger.Verbose("Writing CREATE TRIGGER statements to postdata file")
	triggers := GetTriggers(connection)
	objectCount["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(connection, TYPE_TRIGGER)
	PrintCreateTriggerStatements(postdataFile, globalTOC, triggers, triggerMetadata)

	postdataFile.Close()
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

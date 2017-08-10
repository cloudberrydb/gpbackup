package backup

import (
	"flag"
	"fmt"
	"os"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	connection *utils.DBConn
	logger     *utils.Logger
	version    string
)

var ( // Command-line flags
	dbname       *string
	debug        *bool
	dumpDir      *string
	quiet        *bool
	verbose      *bool
	dumpGlobals  *bool
	printVersion *bool
)

// We define and initialize flags separately to avoid import conflicts in tests
func initializeFlags() {
	dbname = flag.String("dbname", "", "The database to be backed up")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	dumpDir = flag.String("dumpdir", "", "The directory to which all dump files will be written")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	dumpGlobals = flag.Bool("globals", false, "Dump global metadata")
	printVersion = flag.Bool("version", false, "Print version number and exit")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gpbackup", ""))
}

func SetLogger(log *utils.Logger) {
	logger = log
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	initializeFlags()
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup %s\n", version)
		os.Exit(0)
	}
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
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

	utils.SetDumpTimestamp("")

	if *dumpDir != "" {
		utils.BaseDumpDir = *dumpDir
	}
	logger.Verbose("Creating dump directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	utils.SetupSegmentConfiguration(segConfig)
	utils.CreateDumpDirs()
}

func DoBackup() {
	logger.Info("Dump Key = %s", utils.DumpTimestamp)
	logger.Info("Dump Database = %s", utils.QuoteIdent(connection.DBName))
	logger.Info("Database Size = %s", connection.GetDBSize())

	masterDumpDir := utils.GetDirForContent(-1)

	globalFilename := fmt.Sprintf("%s/global.sql", masterDumpDir)
	predataFilename := fmt.Sprintf("%s/predata.sql", masterDumpDir)
	postdataFilename := fmt.Sprintf("%s/postdata.sql", masterDumpDir)

	connection.Begin()
	connection.Exec("SET search_path TO pg_catalog")

	tables := GetAllUserTables(connection)
	tableDefs := ConstructDefinitionsForTables(connection, tables)

	logger.Info("Writing global database metadata to %s", globalFilename)
	backupGlobal(globalFilename)
	logger.Info("Global database metadata dump complete")

	logger.Info("Writing pre-data metadata to %s", predataFilename)
	backupPredata(predataFilename, tables, tableDefs)
	logger.Info("Pre-data metadata dump complete")

	logger.Info("Writing data to file")
	backupData(tables, tableDefs)
	logger.Info("Data dump complete")

	logger.Info("Writing post-data metadata to %s", postdataFilename)
	backupPostdata(postdataFilename, tables)
	logger.Info("Post-data metadata dump complete")

	connection.Commit()
}

func backupGlobal(filename string) {
	globalFile := utils.MustOpenFileForWriting(filename)

	logger.Verbose("Writing session GUCs to global file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(globalFile, gucs)

	logger.Verbose("Writing CREATE TABLESPACE statements to global file")
	tablespaces := GetTablespaces(connection)
	tablespaceMetadata := GetMetadataForObjectType(connection, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(globalFile, tablespaces, tablespaceMetadata)

	logger.Verbose("Writing CREATE DATABASE statement to global file")
	dbnames := GetDatabaseNames(connection)
	dbMetadata := GetMetadataForObjectType(connection, TYPE_DATABASE)
	PrintCreateDatabaseStatement(globalFile, connection.DBName, dbnames, dbMetadata, *dumpGlobals)

	logger.Verbose("Writing database GUCs to global file")
	databaseGucs := GetDatabaseGUCs(connection)
	PrintDatabaseGUCs(globalFile, databaseGucs, connection.DBName)

	logger.Verbose("Writing CREATE RESOURCE QUEUE statements to global file")
	resQueues := GetResourceQueues(connection)
	resQueueMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(globalFile, resQueues, resQueueMetadata)

	logger.Verbose("Writing CREATE ROLE statements to global file")
	roles := GetRoles(connection)
	roleMetadata := GetCommentsForObjectType(connection, TYPE_ROLE)
	PrintCreateRoleStatements(globalFile, roles, roleMetadata)

	logger.Verbose("Writing GRANT ROLE statements to global file")
	roleMembers := GetRoleMembers(connection)
	PrintRoleMembershipStatements(globalFile, roleMembers)
}

func backupPredata(filename string, tables []Relation, tableDefs map[uint32]TableDefinition) {
	predataFile := utils.MustOpenFileForWriting(filename)
	PrintConnectionString(predataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(predataFile, gucs)

	logger.Verbose("Writing CREATE SCHEMA statements to predata file")
	schemas := GetAllUserSchemas(connection)
	schemaMetadata := GetMetadataForObjectType(connection, TYPE_SCHEMA)
	PrintCreateSchemaStatements(predataFile, schemas, schemaMetadata)

	types := GetTypes(connection)
	typeMetadata := GetMetadataForObjectType(connection, TYPE_TYPE)
	functions := GetFunctions(connection)
	funcInfoMap := GetFunctionOidToInfoMap(connection)
	functionMetadata := GetMetadataForObjectType(connection, TYPE_FUNCTION)
	functions, types = ConstructFunctionAndTypeDependencyLists(connection, functions, types)

	logger.Verbose("Writing CREATE TYPE statements for shell types to predata file")
	PrintCreateShellTypeStatements(predataFile, types)

	logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to predata file")
	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(predataFile, langFunc, functionMetadata[langFunc.Oid])
	}
	procLangMetadata := GetMetadataForObjectType(connection, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(predataFile, procLangs, funcInfoMap, procLangMetadata)

	logger.Verbose("Writing CREATE TYPE statements for enum types to predata file")
	PrintCreateEnumTypeStatements(predataFile, types, typeMetadata)

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)
	logger.Verbose("Writing CREATE SEQUENCE statements to predata file")
	sequenceDefs := GetAllSequences(connection)
	PrintCreateSequenceStatements(predataFile, sequenceDefs, relationMetadata)

	logger.Verbose("Writing CREATE TABLE statements to predata file")
	tables = ConstructTableDependencies(connection, tables)

	constraints := GetConstraints(connection)
	conMetadata := GetCommentsForObjectType(connection, TYPE_CONSTRAINT)

	logger.Verbose("Writing CREATE FUNCTION statements and CREATE TYPE statements for base, composite, and domain types to predata file")
	sortedSlice := SortFunctionsAndTypesAndTablesInDependencyOrder(otherFuncs, types, tables)
	filteredMetadata := ConstructFunctionAndTypeAndTableMetadataMap(functionMetadata, typeMetadata, relationMetadata)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(predataFile, sortedSlice, filteredMetadata, tableDefs, constraints)

	logger.Verbose("Writing ALTER SEQUENCE statements to predata file")
	sequenceColumnOwners := GetSequenceColumnOwnerMap(connection)
	PrintAlterSequenceStatements(predataFile, sequenceDefs, sequenceColumnOwners)

	logger.Verbose("Writing CREATE TEXT SEARCH PARSER statements to predata file")
	parsers := GetTextSearchParsers(connection)
	parserMetadata := GetCommentsForObjectType(connection, TYPE_TSPARSER)
	PrintCreateTextSearchParserStatements(predataFile, parsers, parserMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH TEMPLATE statements to predata file")
	templates := GetTextSearchTemplates(connection)
	templateMetadata := GetCommentsForObjectType(connection, TYPE_TSTEMPLATE)
	PrintCreateTextSearchTemplateStatements(predataFile, templates, templateMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH DICTIONARY statements to predata file")
	dictionaries := GetTextSearchDictionaries(connection)
	dictionaryMetadata := GetMetadataForObjectType(connection, TYPE_TSDICTIONARY)
	PrintCreateTextSearchDictionaryStatements(predataFile, dictionaries, dictionaryMetadata)

	logger.Verbose("Writing CREATE TEXT SEARCH CONFIGURATION statements to predata file")
	configurations := GetTextSearchConfigurations(connection)
	configurationMetadata := GetMetadataForObjectType(connection, TYPE_TSCONFIGURATION)
	PrintCreateTextSearchConfigurationStatements(predataFile, configurations, configurationMetadata)

	logger.Verbose("Writing CREATE PROTOCOL statements to predata file")
	protocols := GetExternalProtocols(connection)
	protoMetadata := GetMetadataForObjectType(connection, TYPE_PROTOCOL)
	PrintCreateExternalProtocolStatements(predataFile, protocols, funcInfoMap, protoMetadata)

	logger.Verbose("Writing CREATE CONVERSION statements to predata file")
	conversions := GetConversions(connection)
	convMetadata := GetMetadataForObjectType(connection, TYPE_CONVERSION)
	PrintCreateConversionStatements(predataFile, conversions, convMetadata)

	logger.Verbose("Writing CREATE OPERATOR statements to predata file")
	operators := GetOperators(connection)
	operatorMetadata := GetMetadataForObjectType(connection, TYPE_OPERATOR)
	PrintCreateOperatorStatements(predataFile, operators, operatorMetadata)

	logger.Verbose("Writing CREATE OPERATOR FAMILY statements to predata file")
	operatorFamilies := GetOperatorFamilies(connection)
	operatorFamilyMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(predataFile, operatorFamilies, operatorFamilyMetadata)

	logger.Verbose("Writing CREATE OPERATOR CLASS statements to predata file")
	operatorClasses := GetOperatorClasses(connection)
	operatorClassMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORCLASS)
	PrintCreateOperatorClassStatements(predataFile, operatorClasses, operatorClassMetadata)

	logger.Verbose("Writing CREATE AGGREGATE statements to predata file")
	aggDefs := GetAggregates(connection)
	aggMetadata := GetMetadataForObjectType(connection, TYPE_AGGREGATE)
	PrintCreateAggregateStatements(predataFile, aggDefs, funcInfoMap, aggMetadata)

	logger.Verbose("Writing CREATE CAST statements to predata file")
	castDefs := GetCasts(connection)
	castMetadata := GetCommentsForObjectType(connection, TYPE_CAST)
	PrintCreateCastStatements(predataFile, castDefs, castMetadata)

	logger.Verbose("Writing CREATE VIEW statements to predata file")
	views := GetViews(connection)
	views = ConstructViewDependencies(connection, views)
	views = SortViews(views)
	PrintCreateViewStatements(predataFile, views, relationMetadata)

	logger.Verbose("Writing ADD CONSTRAINT statements to predata file")
	PrintConstraintStatements(predataFile, constraints, conMetadata)
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	numExtTables := 0
	for _, table := range tables {
		if !tableDefs[table.RelationOid].IsExternal {
			logger.Verbose("Writing data for table %s to file", table.ToString())
			dumpFile := utils.GetTableDumpFilePath(table.RelationOid)
			CopyTableOut(connection, table, dumpFile)
		} else {
			logger.Verbose("Skipping data dump of table %s because it is an external table.", table.ToString())
			numExtTables++
		}
	}
	if numExtTables > 0 {
		s := ""
		if numExtTables > 1 {
			s = "s"
		}
		logger.Warn("Skipped data dump of %d external table%s.", numExtTables, s)
		logger.Warn("See %s for a complete list of skipped tables.", logger.GetLogFileName())
	}
	logger.Verbose("Writing table map file to %s", utils.GetTableMapFilePath())
	WriteTableMapFile(tables, tableDefs)
}

func backupPostdata(filename string, tables []Relation) {
	postdataFile := utils.MustOpenFileForWriting(filename)
	PrintConnectionString(postdataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(postdataFile, gucs)

	logger.Verbose("Writing CREATE INDEX statements to postdata file")
	indexNameMap := ConstructImplicitIndexNames(connection)
	indexes := GetIndexes(connection, indexNameMap)
	indexMetadata := GetCommentsForObjectType(connection, TYPE_INDEX)
	PrintCreateIndexStatements(postdataFile, indexes, indexMetadata)

	logger.Verbose("Writing CREATE RULE statements to postdata file")
	rules := GetRules(connection)
	ruleMetadata := GetCommentsForObjectType(connection, TYPE_RULE)
	PrintCreateRuleStatements(postdataFile, rules, ruleMetadata)

	logger.Verbose("Writing CREATE TRIGGER statements to postdata file")
	triggers := GetTriggers(connection)
	triggerMetadata := GetCommentsForObjectType(connection, TYPE_TRIGGER)
	PrintCreateTriggerStatements(postdataFile, triggers, triggerMetadata)
}

func DoTeardown() {
	if r := recover(); r != nil {
		fmt.Println(r)
	}
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}

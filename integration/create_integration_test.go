package integration

import (
	"bytes"
	"os"
	"regexp"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("creates a non public schema", func() {
			schemas := []utils.Schema{{0, "test_schema"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(buffer, schemas, schemaMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(2))
			Expect(resultSchemas[0].Name).To(Equal("public"))

			testutils.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[1], "Oid")
		})

		It("modifies the public schema", func() {
			schemas := []utils.Schema{{2200, "public"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(buffer, schemas, schemaMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO gpadmin")
			defer testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[0])
		})
	})

	Describe("PrintTypeStatements", func() {
		var (
			shellType         backup.TypeDefinition
			baseType          backup.TypeDefinition
			compositeTypeAtt1 backup.TypeDefinition
			compositeTypeAtt2 backup.TypeDefinition
			enumType          backup.TypeDefinition
			types             []backup.TypeDefinition
			typeMetadataMap   utils.MetadataMap
		)
		BeforeEach(func() {
			shellType = backup.TypeDefinition{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseType = backup.TypeDefinition{
				Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "-",
				Send: "-", ModIn: "-", ModOut: "-", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Delimiter: ";",
			}
			compositeTypeAtt1 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att1", AttType: "text",
			}
			compositeTypeAtt2 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att2", AttType: "integer",
			}
			enumType = backup.TypeDefinition{
				Type: "e", TypeSchema: "public", TypeName: "enum_type", EnumLabels: "'enum_labels'"}
			types = []backup.TypeDefinition{shellType, baseType, compositeTypeAtt1, compositeTypeAtt2, enumType}
			typeMetadataMap = utils.MetadataMap{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintShellTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(2))
			Expect(resultTypes[0].TypeName).To(Equal("base_type"))
			Expect(resultTypes[1].TypeName).To(Equal("shell_type"))
		})

		It("creates composite and enum types", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, types, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(3))
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt1, &resultTypes[0], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt2, &resultTypes[1], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&enumType, &resultTypes[2], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			backup.PrintCreateBaseTypeStatements(buffer, types, typeMetadataMap)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
	})

	Describe("PrintCreateViewStatements", func() {
		It("creates a view with privileges and a comment (can't specify owner in GPDB5)", func() {
			viewDef := backup.QueryViewDefinition{1, "public", "simpleview", "SELECT pg_roles.rolname FROM pg_roles;"}
			viewMetadataMap := testutils.DefaultMetadataMap("VIEW", true, true, true)
			viewMetadata := viewMetadataMap[1]

			backup.PrintCreateViewStatements(buffer, []backup.QueryViewDefinition{viewDef}, viewMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			resultViews := backup.GetViewDefinitions(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")

			viewDef.Oid = backup.OidFromObjectName(connection, "simpleview", "relname", "pg_class")
			Expect(len(resultViews)).To(Equal(1))
			resultMetadata := resultMetadataMap[viewDef.Oid]
			testutils.ExpectStructsToMatch(&viewDef, &resultViews[0])
			testutils.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
	})

	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {"pg_catalog.plpgsql_validator", "oid", true},
				2: {"pg_catalog.plpgsql_inline_handler", "internal", true},
				3: {"pg_catalog.plpgsql_call_handler", "", true},
				4: {"pg_catalog.plperl_validator", "oid", true},
				5: {"pg_catalog.plperl_inline_handler", "internal", true},
				6: {"pg_catalog.plperl_call_handler", "", true},
			}
			plpgsqlInfo := backup.QueryProceduralLanguage{0, "plpgsql", "testrole", true, true, 1, 2, 3}
			plperlInfo := backup.QueryProceduralLanguage{1, "plperl", "testrole", true, true, 4, 5, 6}
			procLangs := []backup.QueryProceduralLanguage{plpgsqlInfo, plperlInfo}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)
			langMetadata := langMetadataMap[1]

			backup.PrintCreateLanguageStatements(buffer, procLangs, funcInfoMap, langMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plperl")

			resultProcLangs := backup.GetProceduralLanguages(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "lanacl", "lanowner", "pg_language")

			plperlInfo.Oid = backup.OidFromObjectName(connection, "plperl", "lanname", "pg_language")
			Expect(len(resultProcLangs)).To(Equal(2))
			resultMetadata := resultMetadataMap[plperlInfo.Oid]
			testutils.ExpectStructsToMatchIncluding(&plpgsqlInfo, &resultProcLangs[0], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatchIncluding(&plperlInfo, &resultProcLangs[1], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatch(&langMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateFunctionStatements", func() {
		funcMetadataMap := utils.MetadataMap{}
		It("creates a function with a simple return type", func() {
			addFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{addFunction}, funcMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
		})
		It("creates a function that returns a set", func() {
			appendFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{appendFunction}, funcMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
		})
		It("creates a function that returns a table", func() {
			dupFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
				BinaryPath: "", Arguments: "integer", IdentArgs: "integer", ResultType: "TABLE(f1 integer, f2 text)",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, DataAccess: "c",
				Language: "sql",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{dupFunction}, funcMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION dup(integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		aggregateDef := backup.QueryAggregateDefinition{
			Oid: 1, SchemaName: "public", AggregateName: "agg_prefunc", Arguments: "numeric, numeric",
			IdentArgs: "numeric, numeric", TransitionFunction: 1, PreliminaryFunction: 2, FinalFunction: 0,
			SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", IsOrdered: false,
		}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc_accum", Arguments: "numeric, numeric, numeric"},
			2: {QualifiedName: "public.mypre_accum", Arguments: "numeric, numeric"},
		}
		BeforeEach(func() {
			//Run queries to set up the database state so we can successfully create an aggregate
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
		})
		It("creates a basic aggregate", func() {
			emptyMetadataMap := utils.MetadataMap{}
			backup.PrintCreateAggregateStatements(buffer, []backup.QueryAggregateDefinition{aggregateDef}, funcInfoMap, emptyMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregateDefinitions(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction")
		})
		It("creates an aggregate with an owner and a comment", func() {
			aggMetadata := utils.ObjectMetadata{[]utils.ACL{}, "testrole", "This is an aggregate comment."}
			aggMetadataMap := utils.MetadataMap{1: aggMetadata}
			backup.PrintCreateAggregateStatements(buffer, []backup.QueryAggregateDefinition{aggregateDef}, funcInfoMap, aggMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			oid := backup.OidFromObjectName(connection, "agg_prefunc", "proname", "pg_proc")
			resultAggregates := backup.GetAggregateDefinitions(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "", "proowner", "pg_proc")
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction")
			testutils.ExpectStructsToMatch(&aggMetadata, &resultMetadata)
		})
	})
	Describe("PrintConstraintStatements", func() {
		var (
			testTable        utils.Relation
			tableOid         uint32
			uniqueConstraint backup.QueryConstraint
			pkConstraint     backup.QueryConstraint
			fkConstraint     backup.QueryConstraint
			checkConstraint  backup.QueryConstraint
			conMetadataMap   utils.MetadataMap
		)
		BeforeEach(func() {
			testTable = utils.BasicRelation("public", "testtable")
			uniqueConstraint = backup.QueryConstraint{0, "uniq2", "u", "UNIQUE (a, b)", "public.testtable"}
			pkConstraint = backup.QueryConstraint{0, "constraints_other_table_pkey", "p", "PRIMARY KEY (b)", "public.constraints_other_table"}
			fkConstraint = backup.QueryConstraint{0, "fk1", "f", "FOREIGN KEY (b) REFERENCES constraints_other_table(b)", "public.testtable"}
			checkConstraint = backup.QueryConstraint{0, "check1", "c", "CHECK (a <> 42)", "public.testtable"}
			testutils.AssertQueryRuns(connection, "CREATE TABLE public.testtable(a int, b text) DISTRIBUTED BY (b)")
			tableOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			conMetadataMap = utils.MetadataMap{}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE testtable CASCADE")
		})
		It("creates a unique constraint", func() {
			constraints := []backup.QueryConstraint{uniqueConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a primary key constraint", func() {
			constraints := []backup.QueryConstraint{}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a foreign key constraint", func() {
			constraints := []backup.QueryConstraint{fkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[1], "Oid")
		})
		It("creates a check constraint", func() {
			constraints := []backup.QueryConstraint{checkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates multiple constraints on one table", func() {
			constraints := []backup.QueryConstraint{checkConstraint, uniqueConstraint, fkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(4))
			testutils.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[1], "Oid")
			testutils.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[2], "Oid")
			testutils.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[3], "Oid")
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		var (
			columnOwnerMap      map[string]string
			sequence            utils.Relation
			sequenceDef         backup.Sequence
			sequenceMetadataMap utils.MetadataMap
		)
		BeforeEach(func() {
			sequence = utils.Relation{0, 1, "public", "my_sequence"}
			sequenceDef = backup.Sequence{Relation: sequence}
			columnOwnerMap = map[string]string{}
			sequenceMetadataMap = utils.MetadataMap{}
		})
		It("creates a basic sequence", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a complex sequence", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, LogCnt: 0, IsCycled: false, IsCalled: true}
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a sequence owned by a table column", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence",
				LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			columnOwnerMap["public.my_sequence"] = "sequence_table.a"
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			//Create table that sequence can be owned by
			testutils.AssertQueryRuns(connection, "CREATE TABLE sequence_table(a int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE sequence_table")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a sequence with privileges, owner, and comment", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			sequenceMetadata := utils.ObjectMetadata{[]utils.ACL{utils.DefaultACLWithout("testrole", "SEQUENCE", "UPDATE")}, "testrole", "This is a sequence comment."}
			sequenceMetadataMap[1] = sequenceMetadata
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")
			oid := backup.OidFromObjectName(connection, "my_sequence", "relname", "pg_class")
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
			testutils.ExpectStructsToMatch(&sequenceMetadata, &resultMetadata)
		})
	})
	Describe("PrintSessionGUCs", func() {
		It("prints the default session GUCs", func() {
			gucs := backup.QuerySessionGUCs{ClientEncoding: "UTF8", StdConformingStrings: "on", DefaultWithOids: "off"}

			backup.PrintSessionGUCs(buffer, gucs)

			//We just want to check that these queries run successfully, no setup required
			testutils.AssertQueryRuns(connection, buffer.String())
		})
	})
	Describe("PrintCreateIndexStatements", func() {
		var (
			indexNameMap     map[string]bool
			indexMetadataMap utils.MetadataMap
		)
		BeforeEach(func() {
			indexNameMap = map[string]bool{}
			indexMetadataMap = utils.MetadataMap{}
		})
		It("creates a basic index", func() {
			indexes := []backup.QuerySimpleDefinition{
				{0, "index1", "public", "testtable", "CREATE INDEX index1 ON testtable USING btree (i)"},
			}
			backup.PrintCreateIndexStatements(buffer, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultIndexes := backup.GetIndexDefinitions(connection, indexNameMap)
			Expect(len(resultIndexes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []backup.QuerySimpleDefinition{
				{1, "index1", "public", "testtable", "CREATE INDEX index1 ON testtable USING btree (i)"},
			}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true)
			indexMetadata := indexMetadataMap[1]
			backup.PrintCreateIndexStatements(buffer, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			indexes[0].Oid = backup.OidFromObjectName(connection, "index1", "relname", "pg_class")
			resultIndexes := backup.GetIndexDefinitions(connection, indexNameMap)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "indexrelid", "pg_class", "pg_index")
			resultMetadata := resultMetadataMap[indexes[0].Oid]
			Expect(len(resultIndexes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
			testutils.ExpectStructsToMatch(&resultMetadata, &indexMetadata)
		})
	})
	Describe("PrintCreateRuleStatements", func() {
		var (
			ruleMetadataMap utils.MetadataMap
		)
		BeforeEach(func() {
			ruleMetadataMap = utils.MetadataMap{}
		})
		It("creates a basic rule", func() {
			rules := []backup.QuerySimpleDefinition{
				{0, "update_notify", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"},
			}
			backup.PrintCreateRuleStatements(buffer, rules, ruleMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultRules := backup.GetRuleDefinitions(connection)
			Expect(len(resultRules)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{
				{1, "update_notify", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"},
			}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			ruleMetadata := ruleMetadataMap[1]
			backup.PrintCreateRuleStatements(buffer, rules, ruleMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			rules[0].Oid = backup.OidFromObjectName(connection, "update_notify", "rulename", "pg_rewrite")
			resultRules := backup.GetRuleDefinitions(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_rewrite", "pg_rewrite")
			resultMetadata := resultMetadataMap[rules[0].Oid]
			Expect(len(resultRules)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
			testutils.ExpectStructsToMatch(&resultMetadata, &ruleMetadata)
		})
	})
	Describe("PrintCreateCastStatements", func() {
		var (
			castMetadataMap utils.MetadataMap
		)
		BeforeEach(func() {
			castMetadataMap = utils.MetadataMap{}
		})
		It("prints a basic cast", func() {
			castDef := backup.QueryCastDefinition{0, "text", "integer", "public", "casttoint", "text", "a"}

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCastDefinitions(connection)
			Expect(len(resultCasts)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a cast with a comment", func() {
			castDef := backup.QueryCastDefinition{1, "text", "integer", "public", "casttoint", "text", "a"}
			castMetadataMap = testutils.DefaultMetadataMap("CAST", false, false, true)
			castMetadata := castMetadataMap[1]

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCastDefinitions(connection)
			Expect(len(resultCasts)).To(Equal(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_cast", "pg_cast")
			resultMetadata := resultMetadataMap[resultCasts[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &castMetadata, "Oid")
		})
	})
	Describe("PrintRegularTableCreateStatement", func() {
		var (
			extTableEmpty backup.ExternalTableDefinition
			testTable     utils.Relation
			tableDef      backup.TableDefinition
			/*
			 * We need to construct partitionDef and partTemplateDef piecemeal like this,
			 * or go fmt will remove the trailing whitespace and prevent literal comparison.
			 */
			partitionDef = `PARTITION BY LIST(gender) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ), ` + `
          DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
          )`
			subpartitionDef = `PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_girls_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_girls_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_girls_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_girls_2_prt_other_regions', appendonly=false )
                  ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_boys_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_boys_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_boys_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_boys_2_prt_other_regions', appendonly=false )
                  ), ` + `
          DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_other_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_other_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_other_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_other_2_prt_other_regions', appendonly=false )
                  )
          )`
			partTemplateDef = `ALTER TABLE testtable ` + `
SET SUBPARTITION TEMPLATE  ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
		)
		BeforeEach(func() {
			extTableEmpty = backup.ExternalTableDefinition{-2, -2, "", "ALL_SEGMENTS", "t", "", "", "", 0, "", "", "UTF-8", false}
			testTable = utils.BasicRelation("public", "testtable")
			tableDef = backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ExtTableDef: extTableEmpty}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE public.testtable")
		})
		It("creates a table with no attributes", func() {
			tableDef.ColumnDefs = []backup.ColumnDefinition{}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a basic heap table", func() {
			rowOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "", "", ""}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a complex heap table", func() {
			rowOneDefault := backup.ColumnDefinition{1, "i", false, true, false, "integer", "", "", "42"}
			rowNotNullDefault := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "", "", "'bar'::text"}
			tableDef.DistPolicy = "DISTRIBUTED BY (i, j)"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOneDefault, rowNotNullDefault}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a basic append-optimized column-oriented table", func() {
			rowOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "compresstype=zlib,blocksize=32768,compresslevel=1", "", ""}
			rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "compresstype=zlib,blocksize=32768,compresslevel=1", "", ""}
			tableDef.StorageOpts = "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a one-level partition table", func() {
			rowOne := backup.ColumnDefinition{1, "region", false, false, false, "text", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "gender", false, false, false, "text", "", "", ""}
			tableDef.PartDef = partitionDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a two-level partition table", func() {
			rowOne := backup.ColumnDefinition{1, "region", false, false, false, "text", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "gender", false, false, false, "text", "", "", ""}
			tableDef.PartDef = subpartitionDef
			tableDef.PartTemplateDef = partTemplateDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		var (
			extTableEmpty = backup.ExternalTableDefinition{-2, -2, "", "ALL_SEGMENTS", "t", "", "", "", 0, "", "", "UTF-8", false}
			testTable     = utils.BasicRelation("public", "testtable")
			tableRow      = backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "", ""}
			tableDef      = backup.TableDefinition{DistPolicy: "DISTRIBUTED BY (i)", ColumnDefs: []backup.ColumnDefinition{tableRow}, ExtTableDef: extTableEmpty}
			tableMetadata utils.ObjectMetadata
		)
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			tableMetadata = utils.ObjectMetadata{Privileges: []utils.ACL{}}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
		})
		It("prints only owner for a table with no comment or column comments", func() {
			tableMetadata.Owner = "testrole"
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultMetadata := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")
			resultTableMetadata := resultMetadata[testTable.RelationOid]
			testutils.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("prints table comment, table owner, and column comments for a table with all three", func() {
			tableMetadata.Owner = "testrole"
			tableMetadata.Comment = "This is a table comment."
			tableDef.ColumnDefs[0].Comment = "This is a column comment."
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
			resultMetadata := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")
			resultTableMetadata := resultMetadata[testTable.RelationOid]
			testutils.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
		})
	})
	Describe("PrintExternalTableCreateStatement", func() {
		var (
			extTable  backup.ExternalTableDefinition
			testTable utils.Relation
			tableDef  backup.TableDefinition
		)
		BeforeEach(func() {
			extTable = backup.ExternalTableDefinition{
				0, backup.FILE, "file://tmp/ext_table_file", "ALL_SEGMENTS",
				"t", "delimiter '	' null '\\N' escape '\\'", "", "",
				0, "", "", "UTF8", false}
			testTable = utils.BasicRelation("public", "testtable")
			tableDef = backup.TableDefinition{IsExternal: true}
			os.Create("/tmp/ext_table_file")
		})
		AfterEach(func() {
			os.Remove("/tmp/ext_table_file")
			testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE testtable")
		})
		It("creates a READABLE EXTERNAL table", func() {
			extTable.Type = backup.READABLE
			extTable.Writable = false
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.GetExternalTableDefinition(connection, testTable.RelationOid)
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatch(&extTable, &resultTableDef)
		})
		It("creates a WRITABLE EXTERNAL table", func() {
			extTable.Type = backup.WRITABLE
			extTable.Writable = true
			extTable.Location = "gpfdist://outputhost:8081/data1.out"
			extTable.Protocol = backup.GPFDIST
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			resultTableDef := backup.GetExternalTableDefinition(connection, testTable.RelationOid)
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatch(&extTable, &resultTableDef)
		})
	})
	Describe("PrintCreateExternalProtocolStatements", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {"public.write_to_s3", "", false},
			2: {"public.read_from_s3", "", false},
		}
		protocolReadOnly := backup.QueryExtProtocol{1, "s3_read", "testrole", true, 2, 0, 0}
		protocolWriteOnly := backup.QueryExtProtocol{1, "s3_write", "testrole", false, 0, 1, 0}
		protocolReadWrite := backup.QueryExtProtocol{1, "s3_read_write", "testrole", false, 2, 1, 0}
		emptyMetadataMap := utils.MetadataMap{}

		It("creates a trusted protocol with a read function, privileges, and an owner", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolReadOnly}
			protoMetadataMap := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, protoMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolReadOnly, &resultExternalProtocols[0], "Oid", "ReadFunction")
		})
		It("creates a protocol with a write function", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolWriteOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolWriteOnly, &resultExternalProtocols[0], "Oid", "WriteFunction")
		})
		It("creates a protocol with a read and write function", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolReadWrite}

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolReadWrite, &resultExternalProtocols[0], "Oid", "ReadFunction", "WriteFunction")
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		It("creates a basic resource queue with a comment", func() {
			basicQueue := backup.QueryResourceQueue{"basicQueue", -1, "32.80", false, "0.00", "medium", "-1", "this is a resource queue comment"}

			backup.PrintCreateResourceQueueStatements(buffer, []backup.QueryResourceQueue{basicQueue})

			// CREATE RESOURCE QUEUE statements can not be part of a multi-command statement, so
			// feed the CREATE RESOURCE QUEUE and COMMENT ON statements separately.
			hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 2)
			testutils.AssertQueryRuns(connection, hunks[0])
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "basicQueue"`)
			testutils.AssertQueryRuns(connection, hunks[1])

			resultResourceQueues := backup.GetResourceQueues(connection)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == "basicQueue" {
					testutils.ExpectStructsToMatch(&basicQueue, &resultQueue)
					return
				}
			}
		})
		It("creates a resource queue with all attributes", func() {
			everythingQueue := backup.QueryResourceQueue{"everythingQueue", 7, "32.80", true, "22.80", "low", "2GB", ""}

			backup.PrintCreateResourceQueueStatements(buffer, []backup.QueryResourceQueue{everythingQueue})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "everythingQueue"`)

			resultResourceQueues := backup.GetResourceQueues(connection)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == "everythingQueue" {
					testutils.ExpectStructsToMatch(&everythingQueue, &resultQueue)
					return
				}
			}
			Fail("didn't find everythingQueue :(")
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		It("creates a basic role ", func() {
			role1 := backup.QueryRole{
				Oid:             0,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "",
				Comment:         "",
				ResQueue:        "pg_default",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}

			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{role1})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP ROLE "role1"`)
			role1.Oid = backup.OidFromObjectName(connection, "role1", "rolname", "pg_roles")

			resultRoles := backup.GetRoles(connection)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					testutils.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a role with all attributes", func() {
			role1 := backup.QueryRole{
				Oid:             0,
				Name:            "role1",
				Super:           false,
				Inherit:         true,
				CreateRole:      true,
				CreateDB:        true,
				CanLogin:        true,
				ConnectionLimit: 4,
				Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
				ValidUntil:      "2099-01-01 08:00:00-00",
				Comment:         "this is a role comment",
				ResQueue:        "pg_default",
				Createrexthttp:  true,
				Createrextgpfd:  true,
				Createwextgpfd:  true,
				Createrexthdfs:  true,
				Createwexthdfs:  true,
				TimeConstraints: []backup.TimeConstraint{
					{
						Oid:       0,
						StartDay:  0,
						StartTime: "13:30:00",
						EndDay:    3,
						EndTime:   "14:30:00",
					}, {
						Oid:       0,
						StartDay:  5,
						StartTime: "00:00:00",
						EndDay:    5,
						EndTime:   "24:00:00",
					},
				},
			}

			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{role1})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP ROLE "role1"`)
			role1.Oid = backup.OidFromObjectName(connection, "role1", "rolname", "pg_roles")

			resultRoles := backup.GetRoles(connection)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					testutils.ExpectStructsToMatchExcluding(&role1, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
})

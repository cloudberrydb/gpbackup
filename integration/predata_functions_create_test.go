package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var toc utils.TOC
	var backupfile *utils.FileWithByteCount
	BeforeEach(func() {
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("PrintCreateFunctionStatement", func() {
		funcMetadata := backup.ObjectMetadata{}
		It("creates a function with a simple return type", func() {
			addFunction := backup.Function{
				SchemaName: "public", FunctionName: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql",
			}

			backup.PrintCreateFunctionStatement(backupfile, &toc, addFunction, funcMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			resultFunctions := backup.GetFunctions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
		})
		It("creates a function that returns a set", func() {
			appendFunction := backup.Function{
				SchemaName: "public", FunctionName: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql",
			}

			backup.PrintCreateFunctionStatement(backupfile, &toc, appendFunction, funcMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")

			resultFunctions := backup.GetFunctions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
		})
		It("creates a function that returns a table", func() {
			dupFunction := backup.Function{
				SchemaName: "public", FunctionName: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
				BinaryPath: "", Arguments: "integer", IdentArgs: "integer", ResultType: "TABLE(f1 integer, f2 text)",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, DataAccess: "c",
				Language: "sql",
			}

			backup.PrintCreateFunctionStatement(backupfile, &toc, dupFunction, funcMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION dup(integer)")

			resultFunctions := backup.GetFunctions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		aggregateDef := backup.Aggregate{
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
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateAggregateStatements(backupfile, &toc, []backup.Aggregate{aggregateDef}, funcInfoMap, emptyMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregates(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction")
		})
		It("creates an aggregate with an owner and a comment", func() {
			aggMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			aggMetadataMap := backup.MetadataMap{1: aggMetadata}
			backup.PrintCreateAggregateStatements(backupfile, &toc, []backup.Aggregate{aggregateDef}, funcInfoMap, aggMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			oid := testutils.OidFromObjectName(connection, "", "agg_prefunc", backup.TYPE_AGGREGATE)
			resultAggregates := backup.GetAggregates(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_AGGREGATE)
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction")
			testutils.ExpectStructsToMatch(&aggMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateCastStatements", func() {
		var (
			castMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			castMetadataMap = backup.MetadataMap{}
		})
		It("prints a basic cast with a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "pg_catalog.int4", FunctionSchema: "public", FunctionName: "casttoint", FunctionArgs: "text", CastContext: "a"}

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(backupfile, &toc, []backup.Cast{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(len(resultCasts)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a basic cast without a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i"}

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION cast_in(cstring) RETURNS casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION cast_out(casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testutils.AssertQueryRuns(connection, "CREATE TYPE casttesttype (INTERNALLENGTH = variable, INPUT = cast_in, OUTPUT = cast_out)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE casttesttype CASCADE")

			backup.PrintCreateCastStatements(backupfile, &toc, []backup.Cast{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS public.casttesttype)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(len(resultCasts)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a cast with a comment", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "pg_catalog.int4", FunctionSchema: "public", FunctionName: "casttoint", FunctionArgs: "text", CastContext: "a"}
			castMetadataMap = testutils.DefaultMetadataMap("CAST", false, false, true)
			castMetadata := castMetadataMap[1]

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(backupfile, &toc, []backup.Cast{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(len(resultCasts)).To(Equal(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)
			resultMetadata := resultMetadataMap[resultCasts[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &castMetadata, "Oid")
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {QualifiedName: "pg_catalog.plpgsql_validator", Arguments: "oid", IsInternal: true},
				2: {QualifiedName: "pg_catalog.plpgsql_inline_handler", Arguments: "internal", IsInternal: true},
				3: {QualifiedName: "pg_catalog.plpgsql_call_handler", Arguments: "", IsInternal: true},
				4: {QualifiedName: "pg_catalog.plperl_validator", Arguments: "oid", IsInternal: true},
				5: {QualifiedName: "pg_catalog.plperl_inline_handler", Arguments: "internal", IsInternal: true},
				6: {QualifiedName: "pg_catalog.plperl_call_handler", Arguments: "", IsInternal: true},
			}
			plpgsqlInfo := backup.ProceduralLanguage{Oid: 0, Name: "plpgsql", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 1, Inline: 2, Validator: 3}
			plperlInfo := backup.ProceduralLanguage{Oid: 1, Name: "plperl", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 4, Inline: 5, Validator: 6}
			procLangs := []backup.ProceduralLanguage{plpgsqlInfo, plperlInfo}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)
			langMetadata := langMetadataMap[1]

			backup.PrintCreateLanguageStatements(backupfile, &toc, procLangs, funcInfoMap, langMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plperl")

			resultProcLangs := backup.GetProceduralLanguages(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_PROCLANGUAGE)

			plperlInfo.Oid = testutils.OidFromObjectName(connection, "", "plperl", backup.TYPE_PROCLANGUAGE)
			Expect(len(resultProcLangs)).To(Equal(2))
			resultMetadata := resultMetadataMap[plperlInfo.Oid]
			testutils.ExpectStructsToMatchIncluding(&plpgsqlInfo, &resultProcLangs[0], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatchIncluding(&plperlInfo, &resultProcLangs[1], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatch(&langMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateConversionStatements", func() {
		It("creates conversions", func() {
			convOne := backup.Conversion{Oid: 1, Schema: "public", Name: "conv_one", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}
			convTwo := backup.Conversion{Oid: 0, Schema: "public", Name: "conv_two", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: true}
			conversions := []backup.Conversion{convOne, convTwo}
			convMetadataMap := testutils.DefaultMetadataMap("CONVERSION", false, true, true)
			convMetadata := convMetadataMap[1]

			backup.PrintCreateConversionStatements(backupfile, &toc, conversions, convMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP CONVERSION conv_one")
			defer testutils.AssertQueryRuns(connection, "DROP CONVERSION conv_two")

			resultConversions := backup.GetConversions(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_CONVERSION)

			convOne.Oid = testutils.OidFromObjectName(connection, "public", "conv_one", backup.TYPE_CONVERSION)
			convTwo.Oid = testutils.OidFromObjectName(connection, "public", "conv_two", backup.TYPE_CONVERSION)
			Expect(len(resultConversions)).To(Equal(2))
			resultMetadata := resultMetadataMap[convOne.Oid]
			testutils.ExpectStructsToMatch(&convOne, &resultConversions[0])
			testutils.ExpectStructsToMatch(&convTwo, &resultConversions[1])
			testutils.ExpectStructsToMatch(&convMetadata, &resultMetadata)
		})
	})
})

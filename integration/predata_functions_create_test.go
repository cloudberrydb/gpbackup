package integration

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
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

			backup.PrintCreateFunctionStatement(buffer, addFunction, funcMetadata)

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

			backup.PrintCreateFunctionStatement(buffer, appendFunction, funcMetadata)

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

			backup.PrintCreateFunctionStatement(buffer, dupFunction, funcMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION dup(integer)")

			resultFunctions := backup.GetFunctions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		aggregateDef := backup.AggregateDefinition{
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
			backup.PrintCreateAggregateStatements(buffer, []backup.AggregateDefinition{aggregateDef}, funcInfoMap, emptyMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregateDefinitions(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction")
		})
		It("creates an aggregate with an owner and a comment", func() {
			aggMetadata := backup.ObjectMetadata{[]backup.ACL{}, "testrole", "This is an aggregate comment."}
			aggMetadataMap := backup.MetadataMap{1: aggMetadata}
			backup.PrintCreateAggregateStatements(buffer, []backup.AggregateDefinition{aggregateDef}, funcInfoMap, aggMetadataMap)

			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			oid := backup.OidFromObjectName(connection, "", "agg_prefunc", backup.TYPE_AGGREGATE)
			resultAggregates := backup.GetAggregateDefinitions(connection)
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
			castDef := backup.CastDefinition{0, "text", "int4", "public", "casttoint", "text", "a"}

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(buffer, []backup.CastDefinition{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCastDefinitions(connection)
			Expect(len(resultCasts)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a basic cast without a function", func() {
			castDef := backup.CastDefinition{0, "text", "casttesttype", "", "", "", "i"}

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION cast_in(cstring) RETURNS casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION cast_out(casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testutils.AssertQueryRuns(connection, "CREATE TYPE casttesttype (INTERNALLENGTH = variable, INPUT = cast_in, OUTPUT = cast_out)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE casttesttype CASCADE")

			backup.PrintCreateCastStatements(buffer, []backup.CastDefinition{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS public.casttesttype)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCastDefinitions(connection)
			Expect(len(resultCasts)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a cast with a comment", func() {
			castDef := backup.CastDefinition{1, "text", "int4", "public", "casttoint", "text", "a"}
			castMetadataMap = testutils.DefaultMetadataMap("CAST", false, false, true)
			castMetadata := castMetadataMap[1]

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")

			backup.PrintCreateCastStatements(buffer, []backup.CastDefinition{castDef}, castMetadataMap)
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCastDefinitions(connection)
			Expect(len(resultCasts)).To(Equal(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)
			resultMetadata := resultMetadataMap[resultCasts[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &castMetadata, "Oid")
		})
	})
})

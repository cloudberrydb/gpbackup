package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		testutils.SetupTestLogger()
	})
	Describe("GetFunctionDefinitions", func() {
		It("returns a slice of function definitions", func() {
			testutils.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")
			testutils.AssertQueryRuns(connection, "COMMENT ON FUNCTION append(integer, integer) IS 'this is a function comment'")

			results := backup.GetFunctionDefinitions(connection)

			addFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql"}
			appendFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql"}

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			testutils.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
		})
	})
	Describe("GetAggregateDefinitions", func() {
		It("returns a slice of aggregate definitions", func() {
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
CREATE AGGREGATE agg_prefunc(numeric, numeric) (
	SFUNC = mysfunc_accum,
	STYPE = numeric,
	PREFUNC = mypre_accum,
	INITCOND = 0 );
`)
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			transitionOid := backup.OidFromObjectName(connection, "mysfunc_accum", backup.FunctionParams)
			prelimOid := backup.OidFromObjectName(connection, "mypre_accum", backup.FunctionParams)

			result := backup.GetAggregateDefinitions(connection)

			aggregateDef := backup.QueryAggregateDefinition{
				SchemaName: "public", AggregateName: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", IsOrdered: false,
			}

			Expect(len(result)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
	})
	Describe("GetFunctionOidToInfoMap", func() {
		It("returns map containing function information", func() {
			result := backup.GetFunctionOidToInfoMap(connection)
			initialLength := len(result)
			testutils.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			result = backup.GetFunctionOidToInfoMap(connection)
			oid := backup.OidFromObjectName(connection, "add", backup.FunctionParams)
			Expect(len(result)).To(Equal(initialLength + 1))
			Expect(result[oid].QualifiedName).To(Equal("public.add"))
			Expect(result[oid].Arguments).To(Equal("integer, integer"))
			Expect(result[oid].IsInternal).To(BeFalse())
		})
		It("returns a map containing an internal function", func() {
			result := backup.GetFunctionOidToInfoMap(connection)

			oid := backup.OidFromObjectName(connection, "boolin", backup.FunctionParams)
			Expect(result[oid].QualifiedName).To(Equal("pg_catalog.boolin"))
			Expect(result[oid].IsInternal).To(BeTrue())
		})
	})
	Describe("GetCastDefinitions", func() {
		It("returns a slice for a basic cast", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")
			testutils.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION casttoint(text) AS ASSIGNMENT")
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")

			results := backup.GetCastDefinitions(connection)

			castDef := backup.QueryCastDefinition{0, "text", "integer", "public", "casttoint", "text", "a"}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice for a basic cast with comment", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")
			testutils.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION casttoint(text) AS ASSIGNMENT")
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")
			testutils.AssertQueryRuns(connection, "COMMENT ON CAST (text AS integer) IS 'this is a cast comment'")

			results := backup.GetCastDefinitions(connection)

			castDef := backup.QueryCastDefinition{1, "text", "integer", "public", "casttoint", "text", "a"}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
	})
})

package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetOperators", func() {
		It("returns a slice of operators", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")

			expectedOperator := backup.Operator{Oid: 0, SchemaName: "public", Name: "##", ProcedureName: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operators with a full-featured operator", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\"(path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\"(path,path)")

			testutils.AssertQueryRuns(connection, `
			CREATE OPERATOR testschema.## (
				LEFTARG = path,
				RIGHTARG = path,
				PROCEDURE = testschema."testFunc",
				COMMUTATOR = OPERATOR(testschema.##),
				NEGATOR = OPERATOR(public.###),
				RESTRICT = eqsel,
				JOIN = eqjoinsel,
				HASHES,
				MERGES
			)`)
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.## (path, path)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ### (path, path)")

			expectedOperator := backup.Operator{Oid: 0, SchemaName: "testschema", Name: "##", ProcedureName: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "testschema.##", NegatorOp: "###", RestrictFunction: "eqsel", JoinFunction: "eqjoinsel", CanHash: true, CanMerge: true}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operators from a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR testschema.## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.## (bigint, NONE)")
			backup.SetIncludeSchemas([]string{"testschema"})

			expectedOperator := backup.Operator{Oid: 0, SchemaName: "testschema", Name: "##", ProcedureName: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorFamilies", func() {
		It("returns a slice of operator families", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")

			expectedOperator := backup.OperatorFamily{Oid: 0, SchemaName: "public", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operator families in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING hash")
			backup.SetIncludeSchemas([]string{"testschema"})

			expectedOperator := backup.OperatorFamily{Oid: 0, SchemaName: "testschema", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorClasses", func() {
		It("returns a slice of operator classes", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING hash AS STORAGE uuid")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash CASCADE")

			expected := backup.OperatorClass{Oid: 0, ClassSchema: "public", ClassName: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with different type and storage type", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING gist;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING gist CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass DEFAULT FOR TYPE uuid USING gist FAMILY testschema.testfam AS STORAGE int")

			expected := backup.OperatorClass{Oid: 0, ClassSchema: "public", ClassName: "testclass", FamilySchema: "testschema", FamilyName: "testfam", IndexMethod: "gist", Type: "uuid", Default: true, StorageType: "integer", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with operators and functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING gist AS OPERATOR 1 = RECHECK, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING gist CASCADE")

			expected := backup.OperatorClass{Oid: 0, ClassSchema: "public", ClassName: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			expected.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(uuid,uuid)", Recheck: true}, {ClassOid: 0, StrategyNumber: 2, Operator: "<(uuid,uuid)", Recheck: false}}
			expected.Functions = []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}, {ClassOid: 0, SupportNumber: 2, FunctionName: "int4out(integer)"}}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
		It("returns a slice of operator classes for a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING hash AS STORAGE uuid")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testschema.testclass FOR TYPE uuid USING hash AS STORAGE uuid")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testclass USING hash CASCADE")
			backup.SetIncludeSchemas([]string{"testschema"})

			expected := backup.OperatorClass{Oid: 0, ClassSchema: "testschema", ClassName: "testclass", FamilySchema: "testschema", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
	})
})

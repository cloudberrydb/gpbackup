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
	Describe("GetOperators", func() {
		It("returns a slice of operators", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")

			expectedOperator := backup.Operator{0, "public", "##", "numeric_fac", "bigint", "-", "0", "0", "-", "-", false, false}

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

			expectedOperator := backup.Operator{0, "testschema", "##", "testschema.\"testFunc\"", "path", "path", "testschema.##", "###", "eqsel", "eqjoinsel", true, true}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorFamilies", func() {
		It("returns a slice of operator families", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")

			expectedOperator := backup.OperatorFamily{0, "public", "testfam", "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorClasses", func() {
		It("returns a slice of operator classes", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING hash AS STORAGE uuid")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash CASCADE")

			expected := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

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

			expected := backup.OperatorClass{0, "public", "testclass", "testschema", "testfam", "gist", "uuid", true, "integer", nil, nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with operators and functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING gist AS OPERATOR 1 = RECHECK, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING gist CASCADE")

			expected := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "gist", "uuid", false, "-", nil, nil}
			expected.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", true}, {0, 2, "<(uuid,uuid)", false}}
			expected.Functions = []backup.OperatorClassFunction{{0, 1, "abs(integer)"}, {0, 2, "int4out(integer)"}}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
	})
})

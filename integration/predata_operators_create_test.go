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
	Describe("PrintCreateOperatorStatements", func() {
		It("creates operator", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operator := backup.Operator{0, "testschema", "##", "testschema.\"testFunc\"", "path", "path", "0", "0", "-", "-", false, false}
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(buffer, operators, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connection)
			Expect(len(resultOperators)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
		})
		It("creates operator with owner and comment", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operatorMetadataMap := testutils.DefaultMetadataMap("OPERATOR", false, false, true)
			operatorMetadata := operatorMetadataMap[1]
			operator := backup.Operator{1, "testschema", "##", "testschema.\"testFunc\"", "path", "path", "0", "0", "-", "-", false, false}
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(buffer, operators, operatorMetadataMap)
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connection)
			Expect(len(resultOperators)).To(Equal(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_OPERATOR)
			resultMetadata := resultMetadataMap[resultOperators[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("creates operator family", func() {
			operatorFamily := backup.OperatorFamily{1, "public", "testfam", "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}

			backup.PrintCreateOperatorFamilyStatements(buffer, operatorFamilies, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connection)
			Expect(len(resultOperatorFamilies)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
		})
		It("creates operator family with owner and comment", func() {
			operatorFamilyMetadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)
			operatorFamilyMetadata := operatorFamilyMetadataMap[1]
			operatorFamily := backup.OperatorFamily{1, "public", "testfam", "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}

			backup.PrintCreateOperatorFamilyStatements(buffer, operatorFamilies, operatorFamilyMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connection)
			Expect(len(resultOperatorFamilies)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORFAMILY)
			resultMetadata := resultMetadataMap[resultOperatorFamilies[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorFamilyMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("creates basic operator class", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.OperatorClass{operatorClass}, nil)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")
		})
		It("creates complex operator class", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testfam", "gist", "uuid", true, "integer", nil, nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", true}}
			operatorClass.Functions = []backup.OperatorClassFunction{{0, 1, "abs(integer)"}}

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY public.testfam USING gist")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING gist CASCADE")
			backup.PrintCreateOperatorClassStatements(buffer, []backup.OperatorClass{operatorClass}, nil)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
		It("creates basic operator class with a comment and owner", func() {
			operatorClassMetadataMap := testutils.DefaultMetadataMap("OPERATOR CLASS", false, true, true)
			operatorClassMetadata := operatorClassMetadataMap[1]

			operatorClass := backup.OperatorClass{1, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.OperatorClass{operatorClass}, operatorClassMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORCLASS)
			resultMetadata := resultMetadataMap[resultOperatorClasses[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorClassMetadata, "Oid")

		})
	})
})

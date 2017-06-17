package integration

import (
	"gpbackup/backup"
	"gpbackup/testutils"

	"bytes"
	"gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {

	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("creates a non public schema", func() {
			schemas := []utils.Schema{{0, "test_schema", "test comment", "testrole"}}

			backup.PrintCreateSchemaStatements(buffer, schemas)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(2))
			Expect(resultSchemas[0].SchemaName).To(Equal("public"))

			testutils.ExpectStructsToMatchExcluding(&resultSchemas[1], &schemas[0], []string{"SchemaOid"})

		})

		It("modifies the public schema", func() {
			schemas := []utils.Schema{{0, "public", "test comment", "testrole"}}

			backup.PrintCreateSchemaStatements(buffer, schemas)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO pivotal")
			defer testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultSchemas[0], &schemas[0], []string{"SchemaOid"})
		})
	})

	Describe("PrintTypeStatements", func() {
		var shellType backup.TypeDefinition
		var baseType backup.TypeDefinition
		var compositeTypeAtt1 backup.TypeDefinition
		var compositeTypeAtt2 backup.TypeDefinition
		var enumType backup.TypeDefinition
		var types []backup.TypeDefinition

		BeforeEach(func() {
			shellType = backup.TypeDefinition{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseType = backup.TypeDefinition{
				Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out",
				Receive: "-", Send: "-", ModIn: "-", ModOut: "-", InternalLength: 4, IsPassedByValue: true, Alignment: "i",
				Storage: "p", DefaultVal: "default", Element: "text", Delimiter: ";", Comment: "base type comment", Owner: "testrole"}
			compositeTypeAtt1 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type", Comment: "comment", Owner: "testrole",
				AttName: "att1", AttType: "text"}
			compositeTypeAtt2 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type", Comment: "comment", Owner: "testrole",
				AttName: "att2", AttType: "integer"}
			enumType = backup.TypeDefinition{
				Type: "e", TypeSchema: "public", TypeName: "enum_type", Comment: "comment", Owner: "testrole", EnumLabels: "'enum_labels'"}
			types = []backup.TypeDefinition{shellType, baseType, compositeTypeAtt1, compositeTypeAtt2, enumType}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintShellTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(2))
			Expect(results[0].TypeName).To(Equal("base_type"))
			Expect(results[1].TypeName).To(Equal("shell_type"))
		})

		It("creates composite and enum types", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(3))
			testutils.ExpectStructsToMatchIncluding(&results[0], &compositeTypeAtt1, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType"})
			testutils.ExpectStructsToMatchIncluding(&results[1], &compositeTypeAtt2, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType"})
			testutils.ExpectStructsToMatchIncluding(&results[2], &enumType, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "EnumLabels"})

		})

		It("creates base types", func() {
			backup.PrintCreateBaseTypeStatements(buffer, types)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal(baseType))
		})
	})
})

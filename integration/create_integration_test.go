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
	It("creates a non public schema", func() {
		schemas := []utils.Schema{utils.Schema{0, "test_schema", "test comment", "testrole"}}
		buffer := bytes.NewBuffer([]byte(""))

		backup.PrintCreateSchemaStatements(buffer, schemas)

		testutils.AssertQueryRuns(connection, buffer.String())
		defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema")

		resultSchemas := backup.GetAllUserSchemas(connection)

		Expect(len(resultSchemas)).To(Equal(2))
		Expect(resultSchemas[0].SchemaName).To(Equal("public"))

		testutils.ExpectStructsToMatch(&resultSchemas[1], &schemas[0], []string{"SchemaOid"})
	})

	It("modifies the public schema", func() {
		schemas := []utils.Schema{utils.Schema{0, "public", "test comment", "testrole"}}
		buffer := bytes.NewBuffer([]byte(""))

		backup.PrintCreateSchemaStatements(buffer, schemas)

		testutils.AssertQueryRuns(connection, buffer.String())
		defer testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO pivotal")
		defer testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA public IS 'standard public schema'")

		resultSchemas := backup.GetAllUserSchemas(connection)

		Expect(len(resultSchemas)).To(Equal(1))
		testutils.ExpectStructsToMatch(&resultSchemas[0], &schemas[0], []string{"SchemaOid"})
	})
})

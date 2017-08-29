package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/backup tests", func() {
	AfterEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		backup.SetSchemaInclude([]string{})
	})
	Describe("ValidateWithConnection", func() {
		It("passes if schemaInclude is not set", func() {
			backup.SetSchemaInclude([]string{})
			backup.ValidateWithConnection(connection)
		})
		It("passes if single schema is present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			single_schema_row := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_schema_row)
			backup.SetSchemaInclude([]string{"schema1"})
			backup.ValidateWithConnection(connection)
		})
		It("passes if multiple schemas are present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1").AddRow("schema2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			backup.SetSchemaInclude([]string{"schema1", "schema2"})
			backup.ValidateWithConnection(connection)
		})
		It("panics if schema is not present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			backup.SetSchemaInclude([]string{"schema1", "schema2"})
			defer testutils.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateWithConnection(connection)
		})
	})
})

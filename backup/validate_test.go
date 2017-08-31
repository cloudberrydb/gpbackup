package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/validate tests", func() {
	var filterList []string
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateFilterSchemas", func() {
		It("passes if there are no filter schemas", func() {
			backup.ValidateFilterSchemas(connection, filterList)
		})
		It("passes if single schema is present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			single_schema_row := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_schema_row)
			filterList = []string{"schema1"}
			backup.ValidateFilterSchemas(connection, filterList)
		})
		It("passes if multiple schemas are present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1").AddRow("schema2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			backup.ValidateFilterSchemas(connection, filterList)
		})
		It("panics if schema is not present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			defer testutils.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateFilterSchemas(connection, filterList)
		})
	})
	Describe("ValidateFilterTables", func() {
		It("passes if there are no filter tables", func() {
			backup.ValidateFilterTables(connection, filterList)
		})
		It("passes if single table is present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			single_table_row := sqlmock.NewRows([]string{"string"}).
				AddRow("public.table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
			filterList = []string{"public.table1"}
			backup.ValidateFilterTables(connection, filterList)
		})
		It("passes if multiple tables are present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_table_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("public.table1").AddRow("public.table2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
			filterList = []string{"public.table1", "public.table2"}
			backup.ValidateFilterTables(connection, filterList)
		})
		It("panics if table is not present in database", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_table_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("public.table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
			filterList = []string{"public.table1", "public.table2"}
			defer testutils.ShouldPanicWithMessage("Table public.table2 does not exist")
			backup.ValidateFilterTables(connection, filterList)
		})
	})
})

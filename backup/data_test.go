package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/data tests", func() {
	Describe("ConstructTableAttributesList", func() {
		It("creates an attribute list for a table with one column", func() {
			columnDefs := []backup.ColumnDefinition{{Name: "a"}}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal("(a)"))
		})
		It("creates an attribute list for a table with multiple columns", func() {
			columnDefs := []backup.ColumnDefinition{{Name: "a"}, {Name: "b"}}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal("(a,b)"))
		})
		It("creates an attribute list for a table with no columns", func() {
			columnDefs := []backup.ColumnDefinition{}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal(""))
		})
	})
	Describe("AddTableDataEntriesToTOC", func() {
		var toc *utils.TOC
		BeforeEach(func() {
			toc = &utils.TOC{}
			backup.SetTOC(toc)
		})
		It("adds an entry for a regular table to the TOC", func() {
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			tableDefs := map[uint32]backup.TableDefinition{1: {ColumnDefs: columnDefs}}
			tables := []backup.Relation{{RelationOid: 1, SchemaName: "public", RelationName: "table"}}
			backup.AddTableDataEntriesToTOC(tables, tableDefs)
			expectedDataEntries := []utils.DataEntry{{"public", "table", 1, "(a)"}}
			Expect(toc.DataEntries).To(Equal(expectedDataEntries))
		})
		It("does not add an entry for an external table to the TOC", func() {
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			tableDefs := map[uint32]backup.TableDefinition{1: {ColumnDefs: columnDefs, IsExternal: true}}
			tables := []backup.Relation{{RelationOid: 1, SchemaName: "public", RelationName: "table"}}
			backup.AddTableDataEntriesToTOC(tables, tableDefs)
			Expect(toc.DataEntries).To(BeNil())
		})
	})
	Describe("CopyTableOut", func() {
		It("will back up a table to its own file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c", DecompressCommand: "gzip -d", Extension: ".gz"})
			testTable := backup.Relation{SchemaOid: 2345, RelationOid: 3456, SchemaName: "public", RelationName: "foo", DependsUpon: nil, Inherits: nil}
			execStr := "COPY public.foo TO PROGRAM 'gzip -c > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connection, testTable, filename)
		})
		It("will back up a table to its own file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			testTable := backup.Relation{SchemaOid: 2345, RelationOid: 3456, SchemaName: "public", RelationName: "foo", DependsUpon: nil, Inherits: nil}
			execStr := "COPY public.foo TO '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connection, testTable, filename)
		})
	})
})

package backup_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/data tests", func() {
	Describe("CopyTableOut", func() {
		It("will dump a table to its own file", func() {
			testTable := backup.Relation{SchemaOid: 2345, RelationOid: 3456, SchemaName: "public", RelationName: "foo", DependsUpon: nil, Inherits: nil}
			execStr := "COPY public.foo TO '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connection, testTable, filename)
		})
	})
	Describe("WriteTableMapFile", func() {
		cluster := testutils.SetDefaultSegmentConfiguration()
		tableOne := backup.Relation{SchemaOid: 0, RelationOid: 1234, SchemaName: "public", RelationName: "foo", DependsUpon: nil, Inherits: nil}
		tableTwo := backup.Relation{SchemaOid: 1, RelationOid: 2345, SchemaName: "public", RelationName: "foo|bar", DependsUpon: nil, Inherits: nil}
		var (
			r         *os.File
			w         *os.File
			tableDefs map[uint32]backup.TableDefinition
		)
		BeforeEach(func() {
			r, w, _ = os.Pipe()
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return w, nil }
			tableDefs = map[uint32]backup.TableDefinition{}
		})
		AfterEach(func() {
			utils.System.OpenFile = os.OpenFile
		})
		It("writes a map file containing one table", func() {
			tables := []backup.Relation{tableOne}
			backup.WriteTableMapFile(cluster.GetTableMapFilePath(), tables, tableDefs)
			w.Close()
			output, _ := ioutil.ReadAll(r)
			testutils.ExpectRegex(string(output), `public.foo: 1234
`)
		})
		It("writes a map file containing multiple tables", func() {
			tables := []backup.Relation{tableOne, tableTwo}
			backup.WriteTableMapFile(cluster.GetTableMapFilePath(), tables, tableDefs)
			w.Close()
			output, _ := ioutil.ReadAll(r)
			testutils.ExpectRegex(string(output), `public.foo: 1234
public."foo|bar": 2345`)
		})
		It("does not write external tables to the map file", func() {
			tables := []backup.Relation{tableOne, tableTwo}
			tableDefs[1] = backup.TableDefinition{IsExternal: true}
			backup.WriteTableMapFile(cluster.GetTableMapFilePath(), tables, tableDefs)
			w.Close()
			output, _ := ioutil.ReadAll(r)
			testutils.ExpectRegex(string(output), `public.foo: 1234`)
		})
	})
})

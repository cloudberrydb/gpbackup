package restore_test

import (
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/data tests", func() {
	Describe("GetTableDataEntriesFromTOC", func() {
		It("constructs a map from all TOC data entries", func() {
			toc := &utils.TOC{}
			toc.DataEntries = []utils.DataEntry{{"public", "table", 1, "(i,j)"}, {"testschema", "testtable", 2, "(a)"}}
			restore.SetTOC(toc)
			tableMap := restore.GetTableDataEntriesFromTOC()
			expectedMap := map[uint32]utils.DataEntry{
				1: {"public", "table", 1, "(i,j)"},
				2: {"testschema", "testtable", 2, "(a)"},
			}
			Expect(tableMap).To(Equal(expectedMap))
		})
	})
	Describe("CopyTableIn", func() {
		It("will restore a table from its own file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c", DecompressCommand: "gzip -d", Extension: ".gz"})
			execStr := `COPY public.foo\(i,j\) FROM PROGRAM 'gzip -d < <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz' WITH CSV DELIMITER ',' ON SEGMENT;`
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename)
		})
		It("will restore a table from its own file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := `COPY public.foo\(i,j\) FROM '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;`
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename)
		})
	})
})

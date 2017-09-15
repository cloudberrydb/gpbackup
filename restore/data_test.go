package restore_test

import (
	"os"

	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/data tests", func() {
	Describe("CopyTableIn", func() {
		It("will restore a table from its own file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c", DecompressCommand: "gzip -d", Extension: ".gz"})
			execStr := "COPY public.foo FROM PROGRAM 'gzip -d < <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connection, "public.foo", filename)
		})
		It("will restore a table from its own file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := "COPY public.foo FROM '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connection, "public.foo", filename)
		})
	})
	Describe("ReadTableMapFile", func() {
		cluster := testutils.SetDefaultSegmentConfiguration()
		tableMapFileContents := []byte(`public.foo: 1234
public."bar%baz": 2345
public."contains: delimiter": 3456`)
		expectedTableMap := map[string]uint32{`public.foo`: 1234, `public."bar%baz"`: 2345, `public."contains: delimiter"`: 3456}

		It("reads a map file containing multiple tables, one containing the map delimiter", func() {
			r, w, _ := os.Pipe()
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return r, nil }
			defer func() { utils.System.OpenFile = os.OpenFile }()
			w.Write(tableMapFileContents)
			w.Close()
			tableMap := restore.ReadTableMapFile(cluster.GetTableMapFilePath())
			Expect(tableMap).To(Equal(expectedTableMap))
		})
	})
})

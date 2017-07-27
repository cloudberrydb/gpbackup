package integration

import (
	"bytes"
	"os"

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
	Describe("PrintExternalTableCreateStatement", func() {
		var (
			extTable  backup.ExternalTableDefinition
			testTable backup.Relation
			tableDef  backup.TableDefinition
		)
		BeforeEach(func() {
			extTable = backup.ExternalTableDefinition{
				0, backup.FILE, "file://tmp/ext_table_file", "ALL_SEGMENTS",
				"t", "delimiter '	' null '\\N' escape '\\'", "", "",
				0, "", "", "UTF8", false}
			testTable = backup.BasicRelation("public", "testtable")
			tableDef = backup.TableDefinition{IsExternal: true}
			os.Create("/tmp/ext_table_file")
		})
		AfterEach(func() {
			os.Remove("/tmp/ext_table_file")
			testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE testtable")
		})
		It("creates a READABLE EXTERNAL table", func() {
			extTable.Type = backup.READABLE
			extTable.Writable = false
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			testTable.RelationOid = backup.OidFromObjectName(connection, "public", "testtable", backup.RelationParams)
			resultTableDef := backup.GetExternalTableDefinition(connection, testTable.RelationOid)
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatch(&extTable, &resultTableDef)
		})
		It("creates a WRITABLE EXTERNAL table", func() {
			extTable.Type = backup.WRITABLE
			extTable.Writable = true
			extTable.Location = "gpfdist://outputhost:8081/data1.out"
			extTable.Protocol = backup.GPFDIST
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			testTable.RelationOid = backup.OidFromObjectName(connection, "public", "testtable", backup.RelationParams)
			resultTableDef := backup.GetExternalTableDefinition(connection, testTable.RelationOid)
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatch(&extTable, &resultTableDef)
		})
	})
	Describe("PrintCreateExternalProtocolStatements", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {"public.write_to_s3", "", false},
			2: {"public.read_from_s3", "", false},
		}
		protocolReadOnly := backup.QueryExtProtocol{1, "s3_read", "testrole", true, 2, 0, 0}
		protocolWriteOnly := backup.QueryExtProtocol{1, "s3_write", "testrole", false, 0, 1, 0}
		protocolReadWrite := backup.QueryExtProtocol{1, "s3_read_write", "testrole", false, 2, 1, 0}
		emptyMetadataMap := backup.MetadataMap{}

		It("creates a trusted protocol with a read function, privileges, and an owner", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolReadOnly}
			protoMetadataMap := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, protoMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolReadOnly, &resultExternalProtocols[0], "Oid", "ReadFunction")
		})
		It("creates a protocol with a write function", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolWriteOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolWriteOnly, &resultExternalProtocols[0], "Oid", "WriteFunction")
		})
		It("creates a protocol with a read and write function", func() {
			externalProtocols := []backup.QueryExtProtocol{protocolReadWrite}

			backup.PrintCreateExternalProtocolStatements(buffer, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolReadWrite, &resultExternalProtocols[0], "Oid", "ReadFunction", "WriteFunction")
		})
	})
})

package integration

import (
	"os"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var toc utils.TOC
	var backupfile *utils.FileWithByteCount
	BeforeEach(func() {
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("PrintExternalTableCreateStatement", func() {
		var (
			extTable  backup.ExternalTableDefinition
			testTable backup.Relation
			tableDef  backup.TableDefinition
		)
		BeforeEach(func() {
			extTable = backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: backup.FILE, Location: "file://tmp/ext_table_file",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTable: "", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/ext_table_file"}}
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
			extTable.ErrTable = "testtable"
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, &toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with LOG ERRORS INTO", func() {
			testutils.SkipIfNot4(connection)
			extTable.Type = backup.READABLE
			extTable.Writable = false
			extTable.ErrTable = "err_table"
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, &toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a WRITABLE EXTERNAL table", func() {
			extTable.Type = backup.WRITABLE
			extTable.Writable = true
			extTable.Location = "gpfdist://outputhost:8081/data1.out"
			extTable.URIs = []string{"gpfdist://outputhost:8081/data1.out"}
			extTable.Protocol = backup.GPFDIST
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, &toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			testutils.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
	})
	Describe("PrintCreateExternalProtocolStatements", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.write_to_s3", Arguments: "", IsInternal: false},
			2: {QualifiedName: "public.read_from_s3", Arguments: "", IsInternal: false},
		}
		protocolReadOnly := backup.ExternalProtocol{Oid: 1, Name: "s3_read", Owner: "testrole", Trusted: true, ReadFunction: 2, WriteFunction: 0, Validator: 0}
		protocolWriteOnly := backup.ExternalProtocol{Oid: 1, Name: "s3_write", Owner: "testrole", Trusted: false, ReadFunction: 0, WriteFunction: 1, Validator: 0}
		protocolReadWrite := backup.ExternalProtocol{Oid: 1, Name: "s3_read_write", Owner: "testrole", Trusted: false, ReadFunction: 2, WriteFunction: 1, Validator: 0}
		emptyMetadataMap := backup.MetadataMap{}

		It("creates a trusted protocol with a read function, privileges, and an owner", func() {
			externalProtocols := []backup.ExternalProtocol{protocolReadOnly}
			protoMetadataMap := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)

			backup.PrintCreateExternalProtocolStatements(backupfile, &toc, externalProtocols, funcInfoMap, protoMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolReadOnly, &resultExternalProtocols[0], "Oid", "ReadFunction")
		})
		It("creates a protocol with a write function", func() {
			externalProtocols := []backup.ExternalProtocol{protocolWriteOnly}

			backup.PrintCreateExternalProtocolStatements(backupfile, &toc, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolWriteOnly, &resultExternalProtocols[0], "Oid", "WriteFunction")
		})
		It("creates a protocol with a read and write function", func() {
			externalProtocols := []backup.ExternalProtocol{protocolReadWrite}

			backup.PrintCreateExternalProtocolStatements(backupfile, &toc, externalProtocols, funcInfoMap, emptyMetadataMap)

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

package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetExternalTablesMap", func() {
		It("returns empty map when there are no external tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			result := backup.GetExternalTablesMap(connection)

			Expect(len(result)).To(Equal(0))
		})
		It("returns map with external tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")

			result := backup.GetExternalTablesMap(connection)

			Expect(result["public.ext_table"]).To(BeTrue())
		})
		// TODO: Add tests for external partitions
	})
	Describe("GetExternalTableDefinitions", func() {
		It("returns a slice for a basic external table definition", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")
			oid := testutils.OidFromObjectName(connection, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connection)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTable: "", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}
			testutils.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for a basic external web table definition", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL WEB TABLE ext_table(i int)
EXECUTE 'hostname'
FORMAT 'TEXT'`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL WEB TABLE ext_table")
			oid := testutils.OidFromObjectName(connection, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connection)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "hostname", RejectLimit: 0, RejectLimitType: "", ErrTable: "", Encoding: "UTF8",
				Writable: false, URIs: nil}

			testutils.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for a complex external table definition", func() {
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'
OPTIONS (foo 'bar')
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")
			oid := testutils.OidFromObjectName(connection, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connection)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "foo 'bar'", Command: "", RejectLimit: 10, RejectLimitType: "p", ErrTable: "ext_table", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}

			testutils.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		// TODO: Add tests for external partitions
	})
	Describe("GetExternalProtocols", func() {
		It("returns a slice for a protocol", func() {
			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")
			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")
			testutils.AssertQueryRuns(connection, "CREATE PROTOCOL s3 (writefunc = write_to_s3, readfunc = read_from_s3);")
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3")

			readFunctionOid := testutils.OidFromObjectName(connection, "public", "read_from_s3", backup.TYPE_FUNCTION)
			writeFunctionOid := testutils.OidFromObjectName(connection, "public", "write_to_s3", backup.TYPE_FUNCTION)

			results := backup.GetExternalProtocols(connection)

			protocolDef := backup.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: readFunctionOid, WriteFunction: writeFunctionOid, Validator: 0}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&protocolDef, &results[0], "Oid")
		})
	})
})

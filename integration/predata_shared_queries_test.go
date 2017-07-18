package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		testutils.SetupTestLogger()
	})
	Describe("GetMetadataForObjectType", func() {
		It("returns a slice of metadata with modified privileges", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "REVOKE DELETE ON TABLE foo FROM testrole")
			testutils.AssertQueryRuns(connection, "CREATE TABLE bar(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE bar")
			testutils.AssertQueryRuns(connection, "REVOKE ALL ON TABLE bar FROM testrole")
			testutils.AssertQueryRuns(connection, "CREATE TABLE baz(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE baz")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON TABLE baz TO gpadmin")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")

			fooOid := backup.OidFromObjectName(connection, "foo", "relname", "pg_class")
			barOid := backup.OidFromObjectName(connection, "bar", "relname", "pg_class")
			bazOid := backup.OidFromObjectName(connection, "baz", "relname", "pg_class")
			expectedFoo := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithout("testrole", "TABLE", "DELETE")}, Owner: "testrole"}
			expectedBar := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
			expectedBaz := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLForType("gpadmin", "TABLE"), testutils.DefaultACLForType("testrole", "TABLE")}, Owner: "testrole"}
			Expect(len(resultMetadataMap)).To(Equal(3))
			resultFoo := resultMetadataMap[fooOid]
			resultBar := resultMetadataMap[barOid]
			resultBaz := resultMetadataMap[bazOid]
			testutils.ExpectStructsToMatch(&resultFoo, &expectedFoo)
			testutils.ExpectStructsToMatch(&resultBar, &expectedBar)
			testutils.ExpectStructsToMatch(&resultBaz, &expectedBaz)
		})
		It("returns a slice of default metadata for a database", func() {
			testutils.AssertQueryRuns(connection, "GRANT ALL ON DATABASE testdb TO gpadmin")
			testutils.AssertQueryRuns(connection, "COMMENT ON DATABASE testdb IS 'This is a database comment.'")
			expectedMetadata := backup.ObjectMetadata{[]backup.ACL{
				{Grantee: "gpadmin", Create: true, CreateTemp: true, Connect: true},
				{Grantee: "", CreateTemp: true, Connect: true},
			}, "gpadmin", "This is a database comment."}

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "datacl", "datdba", "pg_database")

			oid := backup.OidFromObjectName(connection, "testdb", "datname", "pg_database")
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON TABLE testtable TO testrole")
			testutils.AssertQueryRuns(connection, "COMMENT ON TABLE testtable IS 'This is a table comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")

			oid := backup.OidFromObjectName(connection, "testtable", "relname", "pg_class")
			expectedMetadata := testutils.DefaultMetadataMap("TABLE", true, true, true)[1]
			Expect(len(resultMetadataMap)).To(Equal(1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a sequence", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE testsequence")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE testsequence")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON SEQUENCE testsequence TO testrole")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE testsequence IS 'This is a sequence comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")

			oid := backup.OidFromObjectName(connection, "testsequence", "relname", "pg_class")
			expectedMetadata := testutils.DefaultMetadataMap("SEQUENCE", true, true, true)[1]
			Expect(len(resultMetadataMap)).To(Equal(1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a function", func() {
			testutils.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON FUNCTION add(integer, integer) TO testrole")
			testutils.AssertQueryRuns(connection, "REVOKE ALL ON FUNCTION add(integer, integer) FROM PUBLIC")
			testutils.AssertQueryRuns(connection, "COMMENT ON FUNCTION add(integer, integer) IS 'This is a function comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "pronamespace", "proacl", "proowner", "pg_proc")

			oid := backup.OidFromObjectName(connection, "add", "proname", "pg_proc")
			expectedMetadata := testutils.DefaultMetadataMap("FUNCTION", true, true, true)[1]
			Expect(len(resultMetadataMap)).To(Equal(1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a view", func() {
			testutils.AssertQueryRuns(connection, `CREATE VIEW testview AS SELECT * FROM pg_class`)
			defer testutils.AssertQueryRuns(connection, "DROP VIEW testview")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON testview TO testrole")
			testutils.AssertQueryRuns(connection, "COMMENT ON VIEW testview IS 'This is a view comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "relnamespace", "relacl", "relowner", "pg_class")

			oid := backup.OidFromObjectName(connection, "testview", "relname", "pg_class")
			expectedMetadata := testutils.DefaultMetadataMap("VIEW", true, true, true)[1]
			Expect(len(resultMetadataMap)).To(Equal(1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a schema", func() {
			testutils.AssertQueryRuns(connection, `CREATE SCHEMA testschema`)
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON SCHEMA testschema TO testrole")
			testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA testschema IS 'This is a schema comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "nspacl", "nspowner", "pg_namespace")

			oid := backup.OidFromObjectName(connection, "testschema", "nspname", "pg_namespace")
			expectedMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)[1]
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for an aggregate", func() {
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, `CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
			testutils.AssertQueryRuns(connection, "COMMENT ON AGGREGATE agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "", "proowner", "pg_proc")

			oid := backup.OidFromObjectName(connection, "agg_prefunc", "proname", "pg_proc")
			expectedMetadata := testutils.DefaultMetadataMap("AGGREGATE", false, true, true)[1]
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a type", func() {
			testutils.AssertQueryRuns(connection, `CREATE TYPE testtype AS (name text, num numeric)`)
			defer testutils.AssertQueryRuns(connection, "DROP TYPE testtype")
			testutils.AssertQueryRuns(connection, "COMMENT ON TYPE testtype IS 'This is a type comment.'")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "typnamespace", "", "typowner", "pg_type")

			oid := backup.OidFromObjectName(connection, "testtype", "typname", "pg_type")
			expectedMetadata := testutils.DefaultMetadataMap("TYPE", false, true, true)[1]
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for an external protocol", func() {
			testutils.AssertQueryRuns(connection, `CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")
			testutils.AssertQueryRuns(connection, `CREATE TRUSTED PROTOCOL s3_read (readfunc = public.read_from_s3);`)
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")
			testutils.AssertQueryRuns(connection, "GRANT ALL ON PROTOCOL s3_read TO testrole")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, "", "ptcacl", "ptcowner", "pg_extprotocol")

			oid := backup.OidFromObjectName(connection, "s3_read", "ptcname", "pg_extprotocol")
			expectedMetadata := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)[1]
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
	})
	Describe("GetCommentsForObjectType", func() {
		It("returns a slice of default metadata for an index", func() {
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "indexrelid", "pg_class", "pg_index")
			numIndexes := len(resultMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE TABLE testtable(i int)`)
			testutils.AssertQueryRuns(connection, `CREATE INDEX testindex ON testtable USING btree(i)`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
			testutils.AssertQueryRuns(connection, "COMMENT ON INDEX testindex IS 'This is an index comment.'")

			resultMetadataMap = backup.GetCommentsForObjectType(connection, "", "indexrelid", "pg_class", "pg_index")

			oid := backup.OidFromObjectName(connection, "testindex", "relname", "pg_class")
			expectedMetadataMap := testutils.DefaultMetadataMap("INDEX", false, false, true)
			expectedMetadata := expectedMetadataMap[1]

			Expect(len(resultMetadataMap)).To(Equal(numIndexes + 1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a rule", func() {
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_rewrite", "pg_rewrite")
			numRules := len(resultMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE TABLE testtable(i int)`)
			testutils.AssertQueryRuns(connection, `CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
			testutils.AssertQueryRuns(connection, "COMMENT ON RULE update_notify IS 'This is a rule comment.'")

			resultMetadataMap = backup.GetCommentsForObjectType(connection, "", "oid", "pg_rewrite", "pg_rewrite")

			oid := backup.OidFromObjectName(connection, "update_notify", "rulename", "pg_rewrite")
			expectedMetadataMap := testutils.DefaultMetadataMap("RULE", false, false, true)
			expectedMetadata := expectedMetadataMap[1]

			Expect(len(resultMetadataMap)).To(Equal(numRules + 1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a trigger", func() {
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_trigger", "pg_trigger")
			numTriggers := len(resultMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE TABLE testtable(i int)`)
			testutils.AssertQueryRuns(connection, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
			testutils.AssertQueryRuns(connection, "COMMENT ON TRIGGER sync_testtable ON public.testtable IS 'This is a trigger comment.'")

			resultMetadataMap = backup.GetCommentsForObjectType(connection, "", "oid", "pg_trigger", "pg_trigger")

			oid := backup.OidFromObjectName(connection, "sync_testtable", "tgname", "pg_trigger")
			expectedMetadataMap := testutils.DefaultMetadataMap("TRIGGER", false, false, true)
			expectedMetadata := expectedMetadataMap[1]

			Expect(len(resultMetadataMap)).To(Equal(numTriggers + 1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a cast", func() {
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_cast", "pg_cast")
			numCasts := len(resultMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text) CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE CAST (text AS int) WITH FUNCTION casttoint(text) AS ASSIGNMENT;")
			testutils.AssertQueryRuns(connection, "COMMENT ON CAST (text AS int) IS 'This is a cast comment.'")

			resultMetadataMap = backup.GetCommentsForObjectType(connection, "", "oid", "pg_cast", "pg_cast")

			textOid := backup.OidFromObjectName(connection, "text", "typname", "pg_type")
			intOid := backup.OidFromObjectName(connection, "int4", "typname", "pg_type")
			oid := testutils.OidFromCast(connection, textOid, intOid)
			expectedMetadataMap := testutils.DefaultMetadataMap("CAST", false, false, true)
			expectedMetadata := expectedMetadataMap[1]

			Expect(len(resultMetadataMap)).To(Equal(numCasts + 1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
		It("returns a slice of default metadata for a constraint", func() {
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_constraint", "pg_constraint")
			numConstraints := len(resultMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE TABLE testtable(i int UNIQUE)`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
			testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT testtable_i_key ON public.testtable IS 'This is a constraint comment.'")

			resultMetadataMap = backup.GetCommentsForObjectType(connection, "", "oid", "pg_constraint", "pg_constraint")

			oid := backup.OidFromObjectName(connection, "testtable_i_key", "conname", "pg_constraint")
			expectedMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
			expectedMetadata := expectedMetadataMap[1]

			Expect(len(resultMetadataMap)).To(Equal(numConstraints + 1))
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
		})
	})
})

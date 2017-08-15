package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/predata_operators tests", func() {
	var toc *utils.TOC
	var backupfile *utils.FileWithByteCount
	BeforeEach(func() {
		toc = &utils.TOC{}
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("PrintCreateOperatorStatements", func() {
		It("prints a basic operator", func() {
			operator := backup.Operator{0, "public", "##", "public.path_inter", "public.path", "public.path", "0", "0", "-", "-", false, false}

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "##", "OPERATOR")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path
);`)
		})
		It("prints a full-featured operator", func() {
			operator := backup.Operator{1, "testschema", "##", "public.path_inter", "public.path", "public.path", "testschema.##", "testschema.###", "eqsel(internal,oid,internal,integer)", "eqjoinsel(internal,oid,internal,smallint)", true, true}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR testschema.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path,
	COMMUTATOR = OPERATOR(testschema.##),
	NEGATOR = OPERATOR(testschema.###),
	RESTRICT = eqsel(internal,oid,internal,integer),
	JOIN = eqjoinsel(internal,oid,internal,smallint),
	HASHES,
	MERGES
);

COMMENT ON OPERATOR testschema.## (public.path, public.path) IS 'This is an operator comment.';


ALTER OPERATOR testschema.## (public.path, public.path) OWNER TO testrole;`)
		})
		It("prints an operator with only a left argument", func() {
			operator := backup.Operator{1, "public", "##", "public.path_inter", "public.path", "-", "0", "0", "-", "-", false, false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path
);

COMMENT ON OPERATOR public.## (public.path, NONE) IS 'This is an operator comment.';


ALTER OPERATOR public.## (public.path, NONE) OWNER TO testrole;`)
		})
		It("prints an operator with only a right argument", func() {
			operator := backup.Operator{1, "public", "##", "public.path_inter", "-", "public.\"PATH\"", "0", "0", "-", "-", false, false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	RIGHTARG = public."PATH"
);

COMMENT ON OPERATOR public.## (NONE, public."PATH") IS 'This is an operator comment.';


ALTER OPERATOR public.## (NONE, public."PATH") OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("prints a basic operator family", func() {
			operatorFamily := backup.OperatorFamily{0, "public", "testfam", "hash"}

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, []backup.OperatorFamily{operatorFamily}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "testfam", "OPERATOR FAMILY")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;`)
		})
		It("prints an operator family with an owner and comment", func() {
			operatorFamily := backup.OperatorFamily{1, "public", "testfam", "hash"}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, []backup.OperatorFamily{operatorFamily}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;

COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.';


ALTER OPERATOR FAMILY public.testfam USING hash OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("prints a basic operator class", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "testclass", "OPERATOR CLASS")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	STORAGE uuid;`)
		})
		It("prints an operator class with default and family", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testfam", "hash", "uuid", true, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY public.testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with class and family in different schemas", func() {
			operatorClass := backup.OperatorClass{0, "schema1", "testclass", "Schema2", "testfam", "hash", "uuid", true, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS schema1.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY "Schema2".testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with an operator", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", false}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid);`)
		})
		It("prints an operator class with two operators and recheck", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", true}, {0, 2, ">(uuid,uuid)", false}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid) RECHECK,
	OPERATOR 2 >(uuid,uuid);`)
		})
		It("prints an operator class with a function", func() {
			operatorClass := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Functions = []backup.OperatorClassFunction{{0, 1, "abs(integer)"}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 abs(integer);`)
		})
	})
})

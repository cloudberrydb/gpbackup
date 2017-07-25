package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_types tests", func() {
	buffer := gbytes.NewBuffer()
	typeMetadataMap := backup.MetadataMap{}

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
		typeMetadataMap = backup.MetadataMap{}
	})
	Describe("PrintCreateEnumTypeStatements", func() {
		enumOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		enumTwo := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}

		It("prints an enum type with multiple attributes", func() {
			backup.PrintCreateEnumTypeStatements(buffer, []backup.TypeDefinition{enumOne}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints an enum type with comment and owner", func() {
			typeMetadataMap = testutils.DefaultMetadataMap("TYPE", false, true, true)
			backup.PrintCreateEnumTypeStatements(buffer, []backup.TypeDefinition{enumTwo}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);


COMMENT ON TYPE public.enum_type IS 'This is a type comment.';


ALTER TYPE public.enum_type OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCompositeTypeStatements", func() {
		compOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "baz", AttType: "text"}
		compThree := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "foo", AttType: "float"}

		It("prints a composite type with one attribute", func() {
			backup.PrintCreateCompositeTypeStatements(buffer, []backup.TypeDefinition{compOne}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			backup.PrintCreateCompositeTypeStatements(buffer, []backup.TypeDefinition{compOne, compTwo, compThree}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	baz text,
	foo float
);`)
		})
		It("prints a composite type with comment and owner", func() {
			typeMetadataMap = testutils.DefaultMetadataMap("TYPE", false, true, true)
			backup.PrintCreateCompositeTypeStatements(buffer, []backup.TypeDefinition{compOne, compThree}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	foo float
);


COMMENT ON TYPE public.composite_type IS 'This is a type comment.';


ALTER TYPE public.composite_type OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateBaseTypeStatements", func() {
		baseSimple := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", false}
		basePartial := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", -1, false, "c", "p", "42", "int4", ",", "", "", false}
		baseFull := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", 16, true, "s", "e", "42", "int4", ",", "", "", false}
		basePermOne := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "d", "m", "", "-", "", "", "", false}
		basePermTwo := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "i", "x", "", "-", "", "", "", false}
		baseCommentOwner := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", false}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseSimple}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePartial}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	DEFAULT = 42,
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with all optional arguments provided", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseFull}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	INTERNALLENGTH = 16,
	PASSEDBYVALUE,
	ALIGNMENT = int2,
	STORAGE = extended,
	DEFAULT = 42,
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with double alignment and main storage", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermOne}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermTwo}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = external
);`)
		})
		It("prints a base type with comment and owner", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseCommentOwner}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
	})
	Describe("PrintCreateShellTypeStatements", func() {
		baseOne := backup.TypeDefinition{1, "public", "base_type1", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", false}
		baseTwo := backup.TypeDefinition{1, "public", "base_type2", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", false}
		compOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type1", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type2", Type: "c", AttName: "bar", AttType: "integer"}
		enumOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		It("prints shell type for only a base type", func() {
			backup.PrintCreateShellTypeStatements(buffer, []backup.TypeDefinition{baseOne, baseTwo, compOne, compTwo, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type1;
CREATE TYPE public.base_type2;`)
		})
	})
	Describe("PrintCreateDomainStatements", func() {
		emptyMetadataMap := backup.MetadataMap{}
		domainOne := testutils.DefaultTypeDefinition("d", "domain1")
		domainOne.DefaultVal = "4"
		domainOne.BaseType = "numeric"
		domainOne.NotNull = true
		domainTwo := testutils.DefaultTypeDefinition("d", "domain2")
		domainTwo.BaseType = "varchar"
		baseOne := testutils.DefaultTypeDefinition("b", "base_type1")
		compOne := testutils.DefaultTypeDefinition("c", "composite_type1")
		enumOne := testutils.DefaultTypeDefinition("e", "enum_type")
		It("prints domain types", func() {
			backup.PrintCreateDomainStatements(buffer, []backup.TypeDefinition{domainOne, domainTwo, baseOne, compOne, enumOne}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 NOT NULL;
CREATE DOMAIN public.domain2 AS varchar;`)
		})
		It("prints a composite type with comment and owner", func() {
			typeMetadataMap = testutils.DefaultMetadataMap("DOMAIN", false, true, true)
			backup.PrintCreateDomainStatements(buffer, []backup.TypeDefinition{domainTwo}, typeMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE DOMAIN public.domain2 AS varchar;


COMMENT ON DOMAIN public.domain2 IS 'This is a domain comment.';


ALTER DOMAIN public.domain2 OWNER TO testrole;`)
		})
	})
})

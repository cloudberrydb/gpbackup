package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_types tests", func() {
	buffer := gbytes.NewBuffer()
	typeMetadata := backup.ObjectMetadata{}
	typeMetadataMap := backup.MetadataMap{}

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
		typeMetadata = backup.ObjectMetadata{}
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
	Describe("PrintCreateCompositeTypeStatement", func() {
		oneAtt := []backup.CompositeTypeAttribute{{"foo", "integer"}}
		twoAtts := []backup.CompositeTypeAttribute{{"foo", "integer"}, {"bar", "text"}}
		compType := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type", Type: "c"}

		It("prints a composite type with one attribute", func() {
			compType.CompositeAtts = oneAtt
			backup.PrintCreateCompositeTypeStatement(buffer, compType, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			compType.CompositeAtts = twoAtts
			backup.PrintCreateCompositeTypeStatement(buffer, compType, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`)
		})
		It("prints a composite type with comment and owner", func() {
			compType.CompositeAtts = twoAtts
			typeMetadata = testutils.DefaultMetadataMap("TYPE", false, true, true)[1]
			backup.PrintCreateCompositeTypeStatement(buffer, compType, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);

COMMENT ON TYPE public.composite_type IS 'This is a type comment.';


ALTER TYPE public.composite_type OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateBaseTypeStatement", func() {
		baseSimple := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "c", "p", "", "", "", "", "", false, nil, nil}
		basePartial := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", -1, false, "c", "p", "42", "int4", ",", "", "", false, nil, nil}
		baseFull := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", 16, true, "s", "e", "42", "int4", ",", "", "", false, nil, nil}
		basePermOne := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "d", "m", "", "", "", "", "", false, nil, nil}
		basePermTwo := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "i", "x", "", "", "", "", "", false, nil, nil}
		baseCommentOwner := backup.TypeDefinition{1, "public", "base_type", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "c", "p", "", "", "", "", "", false, nil, nil}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatement(buffer, baseSimple, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			backup.PrintCreateBaseTypeStatement(buffer, basePartial, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with all optional arguments provided", func() {
			backup.PrintCreateBaseTypeStatement(buffer, baseFull, typeMetadata)
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
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with double alignment and main storage", func() {
			backup.PrintCreateBaseTypeStatement(buffer, basePermOne, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatement(buffer, basePermTwo, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = external
);`)
		})
		It("prints a base type with comment and owner", func() {
			backup.PrintCreateBaseTypeStatement(buffer, baseCommentOwner, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
	})
	Describe("PrintCreateShellTypeStatements", func() {
		baseOne := backup.TypeDefinition{1, "public", "base_type1", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "c", "p", "", "", "", "", "", false, nil, nil}
		baseTwo := backup.TypeDefinition{1, "public", "base_type2", "b", "", "", "input_fn", "output_fn",
			"", "", "", "", -1, false, "c", "p", "", "", "", "", "", false, nil, nil}
		compOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type1", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "composite_type2", Type: "c", AttName: "bar", AttType: "integer"}
		enumOne := backup.TypeDefinition{Oid: 1, TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		It("prints shell type for only a base type", func() {
			backup.PrintCreateShellTypeStatements(buffer, []backup.TypeDefinition{baseOne, baseTwo, compOne, compTwo, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type1;
CREATE TYPE public.base_type2;`)
		})
	})
	Describe("PrintCreateDomainStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		domainOne := testutils.DefaultTypeDefinition("d", "domain1")
		domainOne.DefaultVal = "4"
		domainOne.BaseType = "numeric"
		domainOne.NotNull = true
		domainTwo := testutils.DefaultTypeDefinition("d", "domain2")
		domainTwo.BaseType = "varchar"
		It("prints a basic domain type", func() {
			backup.PrintCreateDomainStatement(buffer, domainOne, emptyMetadata)
			testutils.ExpectRegexp(buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 NOT NULL;`)
		})
		It("prints a domain type with comment and owner", func() {
			typeMetadata = testutils.DefaultMetadataMap("DOMAIN", false, true, true)[1]
			backup.PrintCreateDomainStatement(buffer, domainTwo, typeMetadata)
			testutils.ExpectRegexp(buffer, `CREATE DOMAIN public.domain2 AS varchar;


COMMENT ON DOMAIN public.domain2 IS 'This is a domain comment.';


ALTER DOMAIN public.domain2 OWNER TO testrole;`)
		})
	})
})

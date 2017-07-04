package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintCreateCompositeAndEnumTypeStatements", func() {
		compOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "baz", AttType: "text"}
		compThree := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "foo", AttType: "float"}
		compCommentOwnerOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "bar",
			AttType: "integer", Comment: "This is a type comment.", Owner: "test_role"}
		compCommentOwnerTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "foo",
			AttType: "float", Comment: "This is a type comment.", Owner: "test_role"}
		enumOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		enumTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'", Comment: "This is an enum type comment", Owner: "test_role"}

		It("prints a composite type with one attribute", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne, compTwo, compThree})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	baz text,
	foo float
);`)
		})
		It("prints a composite type with comment and owner", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compCommentOwnerOne, compCommentOwnerTwo})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	foo float
);

COMMENT ON TYPE public.composite_type IS 'This is a type comment.';

ALTER TYPE public.composite_type OWNER TO test_role;`)
		})
		It("prints an enum type with multiple attributes", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints an enum type with comment and owner", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{enumTwo})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);

COMMENT ON TYPE public.enum_type IS 'This is an enum type comment';

ALTER TYPE public.enum_type OWNER TO test_role;`)
		})
		It("prints both an enum type and a composite type", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer
);


CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
	})
	Describe("PrintCreateBaseTypeStatements", func() {
		baseSimple := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		basePartial := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", -1, false, "c", "p", "42", "int4", ",", "", "", ""}
		baseFull := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", 16, true, "s", "e", "42", "int4", ",", "", "", ""}
		basePermOne := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "d", "m", "", "-", "", "", "", ""}
		basePermTwo := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "i", "x", "", "-", "", "", "", ""}
		baseCommentOwner := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "This is a type comment.", "test_role"}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseSimple})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePartial})
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
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseFull})
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
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermTwo})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = external
);`)
		})
		It("prints a base type with comment and owner", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseCommentOwner})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);

COMMENT ON TYPE public.base_type IS 'This is a type comment.';

ALTER TYPE public.base_type OWNER TO test_role;`)
		})
	})
	Describe("PrintShellTypeStatements", func() {
		baseOne := backup.TypeDefinition{"public", "base_type1", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		baseTwo := backup.TypeDefinition{"public", "base_type2", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		compOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type1", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type2", Type: "c", AttName: "bar", AttType: "integer"}
		enumOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		It("prints shell type for only a base type", func() {
			backup.PrintShellTypeStatements(buffer, []backup.TypeDefinition{baseOne, baseTwo, compOne, compTwo, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type1;
CREATE TYPE public.base_type2;`)
		})
	})
})

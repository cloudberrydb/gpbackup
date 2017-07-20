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
	Describe("GetTypeDefinitions", func() {
		var (
			shellType         backup.TypeDefinition
			baseTypeDefault   backup.TypeDefinition
			baseTypeCustom    backup.TypeDefinition
			compositeTypeAtt1 backup.TypeDefinition
			compositeTypeAtt2 backup.TypeDefinition
			compositeTypeAtt3 backup.TypeDefinition
			enumType          backup.TypeDefinition
		)
		BeforeEach(func() {
			shellType = backup.TypeDefinition{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseTypeDefault = backup.TypeDefinition{
				Oid: 1, Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "-",
				Send: "-", ModIn: "-", ModOut: "-", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "-", Delimiter: ",",
			}
			baseTypeCustom = backup.TypeDefinition{
				Oid: 1, Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "-",
				Send: "-", ModIn: "-", ModOut: "-", InternalLength: 8, IsPassedByValue: true, Alignment: "c", Storage: "p",
				DefaultVal: "0", Element: "integer", Delimiter: ";",
			}
			compositeTypeAtt1 = backup.TypeDefinition{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name", AttType: "integer",
			}
			compositeTypeAtt2 = backup.TypeDefinition{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name1", AttType: "integer",
			}
			compositeTypeAtt3 = backup.TypeDefinition{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name2", AttType: "text",
			}
			enumType = backup.TypeDefinition{
				Oid: 1, Type: "e", TypeSchema: "public", TypeName: "enum_type", AttName: "", AttType: "", Input: "enum_in", Output: "enum_out",
				Receive: "enum_recv", Send: "enum_send", ModIn: "-", ModOut: "-", InternalLength: 4, IsPassedByValue: true,
				Alignment: "i", Storage: "p", DefaultVal: "", Element: "-", Delimiter: ",", EnumLabels: "'label1',\n\t'label2',\n\t'label3'",
			}
		})
		It("returns a slice for a shell type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&shellType, &results[0], "TypeSchema", "TypeName", "Type")
		})
		It("returns a slice of composite types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE composite_type AS (name int4, name1 int, name2 text);")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(3))
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt1, &results[0], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt2, &results[1], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt3, &results[2], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
		})
		It("returns a slice for a base type with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeDefault, "Oid")
		})
		It("returns a slice for a base type with custom configuration", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=char, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeCustom, "Oid")
		})
		It("returns a slice for an enum type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE enum_type AS ENUM ('label1','label2','label3')")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &enumType, "Oid")
		})
		It("returns a slice containing information for a mix of types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			testutils.AssertQueryRuns(connection, "CREATE TYPE composite_type AS (name int4, name1 int, name2 text);")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=char, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")
			testutils.AssertQueryRuns(connection, "CREATE TYPE enum_type AS ENUM ('label1','label2','label3')")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(6))
			testutils.ExpectStructsToMatchExcluding(&resultTypes[0], &baseTypeCustom, "Oid")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt1, &resultTypes[1], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt2, &resultTypes[2], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt3, &resultTypes[3], "Type", "TypeSchema", "TypeName", "AttName", "AttType")
			testutils.ExpectStructsToMatchExcluding(&resultTypes[4], &enumType, "Oid")
			testutils.ExpectStructsToMatchIncluding(&shellType, &resultTypes[5], "TypeSchema", "TypeName", "Type")
		})
		It("does not return types for sequences or views", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice for a domain type", func() {
			domainType := backup.TypeDefinition{
				Oid: 1, Type: "d", TypeSchema: "public", TypeName: "domain1", AttName: "", AttType: "", Input: "domain_in", Output: "numeric_out",
				Receive: "domain_recv", Send: "numeric_send", ModIn: "-", ModOut: "-", InternalLength: -1, IsPassedByValue: false,
				Alignment: "i", Storage: "m", DefaultVal: "4", Element: "-", Delimiter: ",", EnumLabels: "", BaseType: "numeric",
			}
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain1 AS numeric DEFAULT 4")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain1")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &domainType, "Oid")
		})
	})
})

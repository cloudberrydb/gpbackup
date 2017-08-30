package integration

import (
	"sort"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetTypes", func() {
		var (
			shellType         backup.Type
			baseTypeDefault   backup.Type
			baseTypeCustom    backup.Type
			compositeTypeAtt1 backup.Type
			compositeTypeAtt2 backup.Type
			compositeTypeAtt3 backup.Type
			enumType          backup.Type
		)
		BeforeEach(func() {
			shellType = backup.Type{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseTypeDefault = backup.Type{
				Oid: 1, Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "", Delimiter: ",",
			}
			baseTypeCustom = backup.Type{
				Oid: 1, Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 8, IsPassedByValue: true, Alignment: "c", Storage: "p",
				DefaultVal: "0", Element: "integer", Delimiter: ";",
			}
			compositeTypeAtt1 = backup.Type{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name", AttType: "integer",
			}
			compositeTypeAtt2 = backup.Type{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name1", AttType: "integer",
			}
			compositeTypeAtt3 = backup.Type{
				Oid: 1, Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "name2", AttType: "text",
			}
			enumType = backup.Type{
				Oid: 1, Type: "e", TypeSchema: "public", TypeName: "enum_type", AttName: "", AttType: "", Input: "enum_in", Output: "enum_out",
				Receive: "enum_recv", Send: "enum_send", ModIn: "", ModOut: "", InternalLength: 4, IsPassedByValue: true,
				Alignment: "i", Storage: "p", DefaultVal: "", Element: "", Delimiter: ",", EnumLabels: "'label1',\n\t'label2',\n\t'label3'",
			}
		})
		It("returns a slice for a shell type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&shellType, &results[0], "TypeSchema", "TypeName", "Type")
		})
		It("returns a slice of composite types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE composite_type AS (name int4, name1 int, name2 text);")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			results := backup.GetTypes(connection)

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

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeDefault, "Oid")
		})
		It("returns a slice for a base type with custom configuration", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=char, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeCustom, "Oid")
		})
		It("returns a slice for an enum type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE enum_type AS ENUM ('label1','label2','label3')")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			results := backup.GetTypes(connection)

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

			resultTypes := backup.GetTypes(connection)

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

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("does not return implicit base or composite types for tables with length > NAMEDATALEN", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong(i int)")
			// The table's name will be truncated to 63 characters upon creation, as will the names of its implicit types
			defer testutils.AssertQueryRuns(connection, "DROP TABLE loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo;")

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice for a domain type", func() {
			domainType := backup.Type{
				Oid: 1, Type: "d", TypeSchema: "public", TypeName: "domain1", AttName: "", AttType: "", Input: "domain_in", Output: "numeric_out",
				Receive: "domain_recv", Send: "numeric_send", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false,
				Alignment: "i", Storage: "m", DefaultVal: "4", Element: "", Delimiter: ",", EnumLabels: "", BaseType: "numeric",
			}
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain1 AS numeric DEFAULT 4")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain1")

			results := backup.GetTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &domainType, "Oid")
		})
		It("returns a slice for a type in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TYPE testschema.shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE testschema.shell_type")
			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetTypes(connection)
			shellTypeOtherSchema := backup.Type{Type: "p", TypeSchema: "testschema", TypeName: "shell_type"}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&shellTypeOtherSchema, &results[0], "TypeSchema", "TypeName", "Type")
		})
	})
	Describe("ConstructCompositeTypeDependencies", func() {
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in2(cstring) RETURNS base_type2 AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out2(base_type2) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type2(INPUT=base_fn_in2, OUTPUT=base_fn_out2)")
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type2 CASCADE")
		})
		It("constructs dependencies correctly for a composite type dependent on one user-defined type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, builtin integer)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			allTypes := backup.GetTypes(connection)
			compType := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "comp_type" {
					compType = typ
					break
				}
			}
			compTypes := []backup.Type{compType}

			compTypes = backup.ConstructCompositeTypeDependencies(connection, compTypes)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(1))
			Expect(compTypes[0].DependsUpon[0]).To(Equal("public.base_type"))
		})
		It("constructs dependencies correctly for a composite type dependent on multiple user-defined types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, base2 base_type2)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			allTypes := backup.GetTypes(connection)
			compType := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "comp_type" {
					compType = typ
					break
				}
			}
			compTypes := []backup.Type{compType}

			compTypes = backup.ConstructCompositeTypeDependencies(connection, compTypes)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(compTypes[0].DependsUpon)
			Expect(compTypes[0].DependsUpon).To(Equal([]string{"public.base_type", "public.base_type2"}))
		})
		It("constructs dependencies correctly for a composite type dependent on the same user-defined type multiple times", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, base2 base_type)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			allTypes := backup.GetTypes(connection)
			compType := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "base_type" {
					compType = typ
					return
				}
			}
			compTypes := []backup.Type{compType}

			compTypes = backup.ConstructCompositeTypeDependencies(connection, compTypes)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(1))
			Expect(compTypes[0].DependsUpon[0]).To(Equal("public.base_type"))
		})
	})
	Describe("ConstructBaseTypeDependencies", func() {
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
		})
		It("constructs dependencies on user-defined functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			allTypes := backup.GetTypes(connection)
			baseType := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "base_type" {
					baseType = typ
					break
				}
			}
			baseTypes := []backup.Type{baseType}

			baseTypes = backup.ConstructBaseTypeDependencies(connection, baseTypes)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out"))
		})
		It("doesn't construct dependencies on built-in functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, TYPMOD_IN=numerictypmodin, TYPMOD_OUT=numerictypmodout)")

			allTypes := backup.GetTypes(connection)
			baseType := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "base_type" {
					baseType = typ
					break
				}
			}
			baseTypes := []backup.Type{baseType}

			baseTypes = backup.ConstructBaseTypeDependencies(connection, baseTypes)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out"))
		})
	})
	Describe("ConstructDomainDependencies", func() {
		It("constructs dependencies on user-defined types", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN parent_domain AS integer")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN parent_domain")
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain_type AS parent_domain")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain_type")

			allTypes := backup.GetTypes(connection)
			domain := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "domain_type" {
					domain = typ
					break
				}
			}
			domains := []backup.Type{domain}

			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(1))
			Expect(len(domains[0].DependsUpon)).To(Equal(1))
			Expect(domains[0].DependsUpon[0]).To(Equal("public.parent_domain"))
		})
		It("doesn't construct dependencies on built-in types", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN parent_domain AS integer")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN parent_domain")

			allTypes := backup.GetTypes(connection)
			domain := backup.Type{}
			for _, typ := range allTypes {
				if typ.TypeName == "base_type" {
					domain = typ
					break
				}
			}
			domains := []backup.Type{domain}

			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(1))
			Expect(domains[0].DependsUpon).To(BeNil())
		})
	})
})

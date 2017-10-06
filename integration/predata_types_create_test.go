package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintTypeStatements", func() {
		var (
			shellType         backup.Type
			baseType          backup.Type
			compositeType     backup.Type
			compositeTypeAtt1 backup.Type
			compositeTypeAtt2 backup.Type
			enumType          backup.Type
			domainType        backup.Type
			types             []backup.Type
			typeMetadata      backup.ObjectMetadata
			typeMetadataMap   backup.MetadataMap
		)
		BeforeEach(func() {
			shellType = backup.Type{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseType = backup.Type{
				Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Delimiter: ";",
			}
			atts := []backup.CompositeTypeAttribute{{AttName: "att1", AttType: "text"}, {AttName: "att2", AttType: "integer"}}
			compositeType = backup.Type{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				CompositeAtts: atts,
			}
			compositeTypeAtt1 = backup.Type{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att1", AttType: "text",
			}
			compositeTypeAtt2 = backup.Type{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att2", AttType: "integer",
			}
			enumType = backup.Type{
				Type: "e", TypeSchema: "public", TypeName: "enum_type", EnumLabels: "'enum_labels'"}
			domainType = testutils.DefaultTypeDefinition("d", "domain_type")
			domainType.BaseType = "numeric"
			domainType.Alignment = "i"
			domainType.Storage = "m"
			domainType.Delimiter = ","
			domainType.Input = "domain_in"
			domainType.Output = "numeric_out"
			domainType.Receive = "domain_recv"
			domainType.Send = "numeric_send"
			domainType.DefaultVal = "5"
			domainType.NotNull = true
			types = []backup.Type{shellType, baseType, compositeType, domainType}
			typeMetadata = backup.ObjectMetadata{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintCreateShellTypeStatements(backupfile, &toc, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			resultTypes := backup.GetNonEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(2))
			Expect(resultTypes[0].TypeName).To(Equal("base_type"))
			Expect(resultTypes[1].TypeName).To(Equal("shell_type"))
		})

		It("creates composite types", func() {
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			resultTypes := backup.GetNonEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(2))
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt1, &resultTypes[0], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt2, &resultTypes[1], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
		})

		It("creates enum types", func() {
			testutils.SkipIf4(connection)
			enums := []backup.Type{enumType}
			backup.PrintCreateEnumTypeStatements(backupfile, toc, enums, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&resultTypes[0], &enumType, "Type", "TypeSchema", "TypeName", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseType, typeMetadata)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetNonEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
		It("creates domain types", func() {
			constraints := []backup.Constraint{}
			backup.PrintCreateDomainStatement(backupfile, toc, domainType, typeMetadata, constraints)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE domain_type")

			resultTypes := backup.GetNonEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&domainType, &resultTypes[0], "Oid")
		})
	})
})

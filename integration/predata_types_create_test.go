package integration

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
	})
	Describe("PrintTypeStatements", func() {
		var (
			shellType         backup.TypeDefinition
			baseType          backup.TypeDefinition
			compositeTypeAtt1 backup.TypeDefinition
			compositeTypeAtt2 backup.TypeDefinition
			enumType          backup.TypeDefinition
			domainType        backup.TypeDefinition
			types             []backup.TypeDefinition
			typeMetadataMap   backup.MetadataMap
		)
		BeforeEach(func() {
			shellType = backup.TypeDefinition{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseType = backup.TypeDefinition{
				Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "-",
				Send: "-", ModIn: "-", ModOut: "-", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Delimiter: ";",
			}
			compositeTypeAtt1 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att1", AttType: "text",
			}
			compositeTypeAtt2 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type",
				AttName: "att2", AttType: "integer",
			}
			enumType = backup.TypeDefinition{
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
			types = []backup.TypeDefinition{shellType, baseType, compositeTypeAtt1, compositeTypeAtt2, enumType, domainType}
			typeMetadataMap = backup.MetadataMap{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintCreateShellTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(2))
			Expect(resultTypes[0].TypeName).To(Equal("base_type"))
			Expect(resultTypes[1].TypeName).To(Equal("shell_type"))
		})

		It("creates composite types", func() {
			backup.PrintCreateCompositeTypeStatements(buffer, types, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(2))
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt1, &resultTypes[0], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
			testutils.ExpectStructsToMatchIncluding(&compositeTypeAtt2, &resultTypes[1], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType")
		})

		It("creates enum types", func() {
			backup.PrintCreateEnumTypeStatements(buffer, types, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&enumType, &resultTypes[0], "Type", "TypeSchema", "TypeName", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			backup.PrintCreateBaseTypeStatements(buffer, types, typeMetadataMap)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
		It("creates domain types", func() {
			backup.PrintCreateDomainStatements(buffer, types, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE domain_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&domainType, &resultTypes[0], "Oid")
		})
	})
})

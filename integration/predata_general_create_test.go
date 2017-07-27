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
	Describe("PrintCreateSchemaStatements", func() {
		It("creates a non public schema", func() {
			schemas := []backup.Schema{{0, "test_schema"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(buffer, schemas, schemaMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(2))
			Expect(resultSchemas[0].Name).To(Equal("public"))

			testutils.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[1], "Oid")
		})

		It("modifies the public schema", func() {
			schemas := []backup.Schema{{2200, "public"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(buffer, schemas, schemaMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO anothertestrole")
			defer testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[0])
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {"pg_catalog.plpgsql_validator", "oid", true},
				2: {"pg_catalog.plpgsql_inline_handler", "internal", true},
				3: {"pg_catalog.plpgsql_call_handler", "", true},
				4: {"pg_catalog.plperl_validator", "oid", true},
				5: {"pg_catalog.plperl_inline_handler", "internal", true},
				6: {"pg_catalog.plperl_call_handler", "", true},
			}
			plpgsqlInfo := backup.QueryProceduralLanguage{0, "plpgsql", "testrole", true, true, 1, 2, 3}
			plperlInfo := backup.QueryProceduralLanguage{1, "plperl", "testrole", true, true, 4, 5, 6}
			procLangs := []backup.QueryProceduralLanguage{plpgsqlInfo, plperlInfo}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)
			langMetadata := langMetadataMap[1]

			backup.PrintCreateLanguageStatements(buffer, procLangs, funcInfoMap, langMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plperl")

			resultProcLangs := backup.GetProceduralLanguages(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.ProcLangParams)

			plperlInfo.Oid = backup.OidFromObjectName(connection, "", "plperl", backup.ProcLangParams)
			Expect(len(resultProcLangs)).To(Equal(2))
			resultMetadata := resultMetadataMap[plperlInfo.Oid]
			testutils.ExpectStructsToMatchIncluding(&plpgsqlInfo, &resultProcLangs[0], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatchIncluding(&plperlInfo, &resultProcLangs[1], "IsPl", "PlTrusted")
			testutils.ExpectStructsToMatch(&langMetadata, &resultMetadata)
		})
	})
	Describe("PrintConstraintStatements", func() {
		var (
			testTable        backup.Relation
			tableOid         uint32
			uniqueConstraint backup.QueryConstraint
			pkConstraint     backup.QueryConstraint
			fkConstraint     backup.QueryConstraint
			checkConstraint  backup.QueryConstraint
			conMetadataMap   backup.MetadataMap
		)
		BeforeEach(func() {
			testTable = backup.BasicRelation("public", "testtable")
			uniqueConstraint = backup.QueryConstraint{0, "uniq2", "u", "UNIQUE (a, b)", "public.testtable", false}
			pkConstraint = backup.QueryConstraint{0, "constraints_other_table_pkey", "p", "PRIMARY KEY (b)", "public.constraints_other_table", false}
			fkConstraint = backup.QueryConstraint{0, "fk1", "f", "FOREIGN KEY (b) REFERENCES constraints_other_table(b)", "public.testtable", false}
			checkConstraint = backup.QueryConstraint{0, "check1", "c", "CHECK (a <> 42)", "public.testtable", false}
			testutils.AssertQueryRuns(connection, "CREATE TABLE public.testtable(a int, b text) DISTRIBUTED BY (b)")
			tableOid = backup.OidFromObjectName(connection, "public", "testtable", backup.RelationParams)
			conMetadataMap = backup.MetadataMap{}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE testtable CASCADE")
		})
		It("creates a unique constraint", func() {
			constraints := []backup.QueryConstraint{uniqueConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a primary key constraint", func() {
			constraints := []backup.QueryConstraint{}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a foreign key constraint", func() {
			constraints := []backup.QueryConstraint{fkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[1], "Oid")
		})
		It("creates a check constraint", func() {
			constraints := []backup.QueryConstraint{checkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates multiple constraints on one table", func() {
			constraints := []backup.QueryConstraint{checkConstraint, uniqueConstraint, fkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(4))
			testutils.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[1], "Oid")
			testutils.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[2], "Oid")
			testutils.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[3], "Oid")
		})
		It("creates a check constraint on a domain", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain1 AS numeric")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain1")
			domainCheckConstraint := backup.QueryConstraint{0, "check1", "c", "CHECK (VALUE <> 42::numeric)", "public.domain1", true}
			constraints := []backup.QueryConstraint{domainCheckConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&domainCheckConstraint, &resultConstraints[0], "Oid")
		})
	})
	Describe("PrintSessionGUCs", func() {
		It("prints the default session GUCs", func() {
			gucs := backup.QuerySessionGUCs{ClientEncoding: "UTF8", StdConformingStrings: "on", DefaultWithOids: "off"}

			backup.PrintSessionGUCs(buffer, gucs)

			//We just want to check that these queries run successfully, no setup required
			testutils.AssertQueryRuns(connection, buffer.String())
		})
	})
})

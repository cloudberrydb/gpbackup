package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
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
	Describe("PrintConstraintStatements", func() {
		var (
			testTable                backup.Relation
			tableOid                 uint32
			uniqueConstraint         backup.Constraint
			pkConstraint             backup.Constraint
			fkConstraint             backup.Constraint
			checkConstraint          backup.Constraint
			partitionCheckConstraint backup.Constraint
			conMetadataMap           backup.MetadataMap
		)
		BeforeEach(func() {
			testTable = backup.BasicRelation("public", "testtable")
			uniqueConstraint = backup.Constraint{0, "uniq2", "u", "UNIQUE (a, b)", "public.testtable", false, false}
			pkConstraint = backup.Constraint{0, "constraints_other_table_pkey", "p", "PRIMARY KEY (b)", "public.constraints_other_table", false, false}
			fkConstraint = backup.Constraint{0, "fk1", "f", "FOREIGN KEY (b) REFERENCES constraints_other_table(b)", "public.testtable", false, false}
			checkConstraint = backup.Constraint{0, "check1", "c", "CHECK (a <> 42)", "public.testtable", false, false}
			partitionCheckConstraint = backup.Constraint{0, "check1", "c", "CHECK (id <> 0)", "public.part", false, true}
			testutils.AssertQueryRuns(connection, "CREATE TABLE public.testtable(a int, b text) DISTRIBUTED BY (b)")
			tableOid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			conMetadataMap = backup.MetadataMap{}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE testtable CASCADE")
		})
		It("creates a unique constraint", func() {
			constraints := []backup.Constraint{uniqueConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a primary key constraint", func() {
			constraints := []backup.Constraint{}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a foreign key constraint", func() {
			constraints := []backup.Constraint{fkConstraint}
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
			constraints := []backup.Constraint{checkConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates multiple constraints on one table", func() {
			constraints := []backup.Constraint{checkConstraint, uniqueConstraint, fkConstraint}
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
		It("doesn't create a check constraint on a domain", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain1 AS numeric")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain1")
			domainCheckConstraint := backup.Constraint{0, "check1", "c", "CHECK (VALUE <> 42::numeric)", "public.domain1", true, false}
			constraints := []backup.Constraint{domainCheckConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			Expect(buffer.String()).To(Equal(""))
		})
		It("creates a check constraint on a parent partition table", func() {
			constraints := []backup.Constraint{partitionCheckConstraint}
			backup.PrintConstraintStatements(buffer, constraints, conMetadataMap)

			testutils.AssertQueryRuns(connection, `CREATE TABLE part (id int, year int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( START (2007) END (2008) EVERY (1),
  DEFAULT PARTITION extra ); `)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part CASCADE")
			testutils.AssertQueryRuns(connection, buffer.String())

			resultConstraints := backup.GetConstraints(connection)

			Expect(len(resultConstraints)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&partitionCheckConstraint, &resultConstraints[0], "Oid")
		})
	})
	Describe("PrintSessionGUCs", func() {
		It("prints the default session GUCs", func() {
			gucs := backup.SessionGUCs{ClientEncoding: "UTF8", StdConformingStrings: "on", DefaultWithOids: "off"}

			backup.PrintSessionGUCs(buffer, gucs)

			//We just want to check that these queries run successfully, no setup required
			testutils.AssertQueryRuns(connection, buffer.String())
		})

	})
})

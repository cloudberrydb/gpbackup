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
	Describe("GetAllUserSchemas", func() {
		It("returns user schema information", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connection)

			schemaBar := backup.Schema{0, "bar"}
			schemaPublic := backup.Schema{2200, "public"}

			Expect(len(schemas)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")
		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			testutils.AssertQueryRuns(connection, "CREATE LANGUAGE plperl")
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plperl")

			pgsqlHandlerOid := backup.OidFromObjectName(connection, "pg_catalog", "plpgsql_call_handler", backup.FunctionParams)
			pgsqlInlineOid := backup.OidFromObjectName(connection, "pg_catalog", "plpgsql_inline_handler", backup.FunctionParams)
			pgsqlValidatorOid := backup.OidFromObjectName(connection, "pg_catalog", "plpgsql_validator", backup.FunctionParams)

			perlHandlerOid := backup.OidFromObjectName(connection, "pg_catalog", "plperl_call_handler", backup.FunctionParams)
			perlInlineOid := backup.OidFromObjectName(connection, "pg_catalog", "plperl_inline_handler", backup.FunctionParams)
			perlValidatorOid := backup.OidFromObjectName(connection, "pg_catalog", "plperl_validator", backup.FunctionParams)

			expectedPlpgsqlInfo := backup.ProceduralLanguage{0, "plpgsql", "testrole", true, true, pgsqlHandlerOid, pgsqlInlineOid, pgsqlValidatorOid}
			expectedPlperlInfo := backup.ProceduralLanguage{1, "plperl", "testrole", true, true, perlHandlerOid, perlInlineOid, perlValidatorOid}

			resultProcLangs := backup.GetProceduralLanguages(connection)

			Expect(len(resultProcLangs)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&expectedPlpgsqlInfo, &resultProcLangs[0], "Oid", "Owner")
			testutils.ExpectStructsToMatchExcluding(&expectedPlperlInfo, &resultProcLangs[1], "Oid", "Owner")
		})
	})
	Describe("GetConversions", func() {
		It("returns a slice of conversions", func() {
			testutils.AssertQueryRuns(connection, "CREATE CONVERSION testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testutils.AssertQueryRuns(connection, "DROP CONVERSION testconv")

			expectedConversion := backup.Conversion{0, "public", "testconv", "LATIN1", "MULE_INTERNAL", "pg_catalog.latin1_to_mic", false}

			resultConversions := backup.GetConversions(connection)

			Expect(len(resultConversions)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
	})
	Describe("GetConstraints", func() {
		var (
			uniqueConstraint         = backup.Constraint{0, "uniq2", "u", "UNIQUE (a, b)", "public.constraints_table", false, false}
			fkConstraint             = backup.Constraint{0, "fk1", "f", "FOREIGN KEY (b) REFERENCES constraints_table(b)", "public.constraints_other_table", false, false}
			pkConstraint             = backup.Constraint{0, "pk1", "p", "PRIMARY KEY (b)", "public.constraints_table", false, false}
			checkConstraint          = backup.Constraint{0, "check1", "c", "CHECK (a <> 42)", "public.constraints_table", false, false}
			partitionCheckConstraint = backup.Constraint{0, "check1", "c", "CHECK (id <> 0)", "public.part", false, true}
		)
		Context("No constraints", func() {
			It("returns an empty constraint array for a table with no constraints", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE no_constraints_table(a int, b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE no_constraints_table")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(0))
			})
		})
		Context("One constraint", func() {
			It("returns a constraint array for a table with one UNIQUE constraint and a comment", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")
			})
			It("returns a constraint array for a table with one PRIMARY KEY constraint and a comment", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one FOREIGN KEY constraint", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table CASCADE")
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES constraints_table(b)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(2))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &fkConstraint, "Oid")
				testutils.ExpectStructsToMatchExcluding(&constraints[1], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one CHECK constraint", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a parent partition table with one CHECK constraint", func() {
				testutils.AssertQueryRuns(connection, `CREATE TABLE part (id int, date date, amt decimal(10,2) default 0.0) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);`)
				defer testutils.AssertQueryRuns(connection, "DROP TABLE part")
				testutils.AssertQueryRuns(connection, "ALTER TABLE part ADD CONSTRAINT check1 CHECK (id <> 0)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &partitionCheckConstraint, "Oid")
			})
		})
		Context("Multiple constraints", func() {
			It("returns a constraint array for a table with multiple constraints", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table CASCADE")
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON constraints_table IS 'this is a constraint comment'")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON constraints_table IS 'this is a constraint comment'")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES constraints_table(b)")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(4))
				testutils.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
				testutils.ExpectStructsToMatchExcluding(&constraints[1], &fkConstraint, "Oid")
				testutils.ExpectStructsToMatchExcluding(&constraints[2], &pkConstraint, "Oid")
				testutils.ExpectStructsToMatchExcluding(&constraints[3], &uniqueConstraint, "Oid")
			})
		})
	})
	Describe("GetOperators", func() {
		It("returns a slice of operators", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")

			expectedOperator := backup.Operator{0, "public", "##", "numeric_fac", "bigint", "-", "0", "0", "-", "-", false, false}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operators with a full-featured operator", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\"(path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\"(path,path)")

			testutils.AssertQueryRuns(connection, `
			CREATE OPERATOR testschema.## (
				LEFTARG = path,
				RIGHTARG = path,
				PROCEDURE = testschema."testFunc",
				COMMUTATOR = OPERATOR(testschema.##),
				NEGATOR = OPERATOR(public.###),
				RESTRICT = eqsel,
				JOIN = eqjoinsel,
				HASHES,
				MERGES
			)`)
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.## (path, path)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ### (path, path)")

			expectedOperator := backup.Operator{0, "testschema", "##", "testschema.\"testFunc\"", "path", "path", "testschema.##", "###", "eqsel", "eqjoinsel", true, true}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorFamilies", func() {
		It("returns a slice of operator families", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")

			expectedOperator := backup.OperatorFamily{0, "public", "testfam", "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorClasses", func() {
		It("returns a slice of operator classes", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING hash AS STORAGE uuid")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash CASCADE")

			expected := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with different type and storage type", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING gist;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING gist CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass DEFAULT FOR TYPE uuid USING gist FAMILY testschema.testfam AS STORAGE int")

			expected := backup.OperatorClass{0, "public", "testclass", "testschema", "testfam", "gist", "uuid", true, "integer", nil, nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with operators and functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE uuid USING gist AS OPERATOR 1 = RECHECK, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING gist CASCADE")

			expected := backup.OperatorClass{0, "public", "testclass", "public", "testclass", "gist", "uuid", false, "-", nil, nil}
			expected.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", true}, {0, 2, "<(uuid,uuid)", false}}
			expected.Functions = []backup.OperatorClassFunction{{0, 1, "abs(integer)"}, {0, 2, "int4out(integer)"}}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
	})
})

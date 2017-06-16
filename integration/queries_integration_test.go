package integration

import (
	"gpbackup/backup"
	"gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetAllUserSchemas", func() {
		It("returns user schema information", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connection)

			Expect(len(schemas)).To(Equal(2))

			Expect(schemas[0].SchemaOid).ToNot(Equal(uint32(0)))
			Expect(schemas[0].SchemaName).To(Equal("bar"))
			Expect(schemas[0].Comment).To(Equal(""))
			Expect(schemas[0].Owner).To(Equal("testrole"))

			Expect(schemas[1].SchemaOid).ToNot(Equal(uint32(0)))
			Expect(schemas[1].SchemaName).To(Equal("public"))
			Expect(schemas[1].Comment).To(Equal("standard public schema"))
			Expect(schemas[1].Owner).ToNot(Equal(""))
		})
	})
	Describe("GetAllUserTables", func() {
		It("returns user table information for basic heap tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(t text)")
			testutils.AssertQueryRuns(connection, "COMMENT ON TABLE public.foo IS 'this is a table comment'")

			tables := backup.GetAllUserTables(connection)

			Expect(len(tables)).To(Equal(2))

			Expect(tables[0].SchemaOid).ToNot(Equal(uint32(0)))
			Expect(tables[0].RelationOid).ToNot(Equal(uint32(0)))
			Expect(tables[0].SchemaName).To(Equal("public"))
			Expect(tables[0].RelationName).To(Equal("foo"))
			Expect(tables[0].Comment).To(Equal("this is a table comment"))
			Expect(tables[0].Owner).To(Equal("testrole"))

			Expect(tables[1].SchemaOid).ToNot(Equal(uint32(0)))
			Expect(tables[1].RelationOid).ToNot(Equal(uint32(0)))
			Expect(tables[1].SchemaName).To(Equal("testschema"))
			Expect(tables[1].RelationName).To(Equal("testtable"))
			Expect(tables[1].Comment).To(Equal(""))
			Expect(tables[1].Owner).To(Equal("testrole"))
		})
		It("only returns the parent partition table for partition tables", func() {
			createStmt := `CREATE TABLE rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
			testutils.AssertQueryRuns(connection, createStmt)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rank")

			tables := backup.GetAllUserTables(connection)

			Expect(len(tables)).To(Equal(1))

			Expect(tables[0].SchemaOid).ToNot(Equal(uint32(0)))
			Expect(tables[0].RelationOid).ToNot(Equal(uint32(0)))
			Expect(tables[0].SchemaName).To(Equal("public"))
			Expect(tables[0].RelationName).To(Equal("rank"))
			Expect(tables[0].Comment).To(Equal(""))
			Expect(tables[0].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetTableAttributes", func() {
		It("returns table attribute information for a heap table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE atttable(a float, b text, c text NOT NULL, d int DEFAULT(5))")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE atttable")
			testutils.AssertQueryRuns(connection, "COMMENT ON COLUMN atttable.a IS 'att comment'")
			testutils.AssertQueryRuns(connection, "ALTER TABLE atttable DROP COLUMN b")
			oid := testutils.OidFromRelationName(connection, "atttable")

			tableAtts := backup.GetTableAttributes(connection, oid)

			Expect(len(tableAtts)).To(Equal(3))

			Expect(tableAtts[0].AttNum).To(Equal(1))
			Expect(tableAtts[0].AttName).To(Equal("a"))
			Expect(tableAtts[0].AttNotNull).To(BeFalse())
			Expect(tableAtts[0].AttHasDefault).To(BeFalse())
			Expect(tableAtts[0].AttIsDropped).To(BeFalse())
			Expect(tableAtts[0].AttTypName).To(Equal("double precision"))
			Expect(tableAtts[0].AttEncoding).To(Equal(""))
			Expect(tableAtts[0].AttComment).To(Equal("att comment"))

			Expect(tableAtts[1].AttNum).To(Equal(3))
			Expect(tableAtts[1].AttName).To(Equal("c"))
			Expect(tableAtts[1].AttNotNull).To(BeTrue())
			Expect(tableAtts[1].AttHasDefault).To(BeFalse())
			Expect(tableAtts[1].AttIsDropped).To(BeFalse())
			Expect(tableAtts[1].AttTypName).To(Equal("text"))
			Expect(tableAtts[1].AttEncoding).To(Equal(""))
			Expect(tableAtts[1].AttComment).To(Equal(""))

			Expect(tableAtts[2].AttNum).To(Equal(4))
			Expect(tableAtts[2].AttName).To(Equal("d"))
			Expect(tableAtts[2].AttNotNull).To(BeFalse())
			Expect(tableAtts[2].AttHasDefault).To(BeTrue())
			Expect(tableAtts[2].AttIsDropped).To(BeFalse())
			Expect(tableAtts[2].AttTypName).To(Equal("integer"))
			Expect(tableAtts[2].AttEncoding).To(Equal(""))
			Expect(tableAtts[2].AttComment).To(Equal(""))
		})
		It("returns table attributes including encoding for a column oriented table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE co_atttable(a float, b text ENCODING(blocksize=65536)) WITH (appendonly=true, orientation=column)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE co_atttable")
			oid := testutils.OidFromRelationName(connection, "co_atttable")

			tableAtts := backup.GetTableAttributes(connection, uint32(oid))

			Expect(len(tableAtts)).To(Equal(2))

			Expect(tableAtts[0].AttNum).To(Equal(1))
			Expect(tableAtts[0].AttName).To(Equal("a"))
			Expect(tableAtts[0].AttNotNull).To(BeFalse())
			Expect(tableAtts[0].AttHasDefault).To(BeFalse())
			Expect(tableAtts[0].AttIsDropped).To(BeFalse())
			Expect(tableAtts[0].AttTypName).To(Equal("double precision"))
			Expect(tableAtts[0].AttEncoding).To(Equal("compresstype=none,blocksize=32768,compresslevel=0"))
			Expect(tableAtts[0].AttComment).To(Equal(""))

			Expect(tableAtts[1].AttNum).To(Equal(2))
			Expect(tableAtts[1].AttName).To(Equal("b"))
			Expect(tableAtts[1].AttNotNull).To(BeFalse())
			Expect(tableAtts[1].AttHasDefault).To(BeFalse())
			Expect(tableAtts[1].AttIsDropped).To(BeFalse())
			Expect(tableAtts[1].AttTypName).To(Equal("text"))
			Expect(tableAtts[1].AttEncoding).To(Equal("blocksize=65536,compresstype=none,compresslevel=0"))
			Expect(tableAtts[1].AttComment).To(Equal(""))
		})
		It("returns an empty attribute array for a table with no columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE nocol_atttable()")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE nocol_atttable")
			oid := testutils.OidFromRelationName(connection, "nocol_atttable")

			tableAtts := backup.GetTableAttributes(connection, uint32(oid))

			Expect(len(tableAtts)).To(Equal(0))
		})
	})
	Describe("GetTableDefaults", func() {
		It("only returns defaults for columns that have them", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE default_table(a text DEFAULT('default text'), b float, c int DEFAULT(5))")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE default_table")
			oid := testutils.OidFromRelationName(connection, "default_table")

			defaults := backup.GetTableDefaults(connection, oid)

			Expect(len(defaults)).To(Equal(2))

			Expect(defaults[0].AdNum).To(Equal(1))
			Expect(defaults[0].DefaultVal).To(Equal("'default text'::text"))

			Expect(defaults[1].AdNum).To(Equal(3))
			Expect(defaults[1].DefaultVal).To(Equal("5"))
		})
		It("returns an empty default array for a table with no defaults", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE nodefault_table(a text, b float, c int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE nodefault_table")
			oid := testutils.OidFromRelationName(connection, "nodefault_table")

			defaults := backup.GetTableDefaults(connection, oid)

			Expect(len(defaults)).To(Equal(0))
		})
	})
	Describe("GetConstraints", func() {
		var (
			uniqueConstraint = backup.QueryConstraint{"uniq2", "u", "UNIQUE (a, b)", "this is a constraint comment"}
			fkConstraint     = backup.QueryConstraint{"fk1", "f", "FOREIGN KEY (b) REFERENCES constraints_other_table(b)", ""}
			pkConstraint     = backup.QueryConstraint{"pk1", "p", "PRIMARY KEY (a, b)", "this is a constraint comment"}
			checkConstraint  = backup.QueryConstraint{"check1", "c", "CHECK (a <> 42)", ""}
		)
		Context("No constraints", func() {
			It("returns an empty constraint array for a table with no constraints", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE no_constraints_table(a int, b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE no_constraints_table")
				oid := testutils.OidFromRelationName(connection, "no_constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(0))
			})
		})
		Context("One constraint", func() {
			It("returns a constraint array for a table with one UNIQUE constraint and a comment", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON constraints_table IS 'this is a constraint comment'")
				oid := testutils.OidFromRelationName(connection, "constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal(uniqueConstraint))
			})

			It("returns a constraint array for a table with one PRIMARY KEY constraint and a comment", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON constraints_table IS 'this is a constraint comment'")
				oid := testutils.OidFromRelationName(connection, "constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal(pkConstraint))

			})

			It("returns a constraint array for a table with one FOREIGN KEY constraint", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table CASCADE")
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_other_table ADD CONSTRAINT uniq1 UNIQUE (b)")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES constraints_other_table(b)")
				oid := testutils.OidFromRelationName(connection, "constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal(fkConstraint))
			})

			It("returns a constraint array for a table with one CHECK constraint", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")
				oid := testutils.OidFromRelationName(connection, "constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal(checkConstraint))
			})
		})
		Context("Multiple constraints", func() {
			It("returns a constraint array for a table with multiple constraints", func() {
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_table(a int, b text, c float)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_table CASCADE")
				testutils.AssertQueryRuns(connection, "CREATE TABLE constraints_other_table(b text)")
				defer testutils.AssertQueryRuns(connection, "DROP TABLE constraints_other_table CASCADE")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_other_table ADD CONSTRAINT uniq1 UNIQUE (b)")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON constraints_table IS 'this is a constraint comment'")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (a, b)")
				testutils.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON constraints_table IS 'this is a constraint comment'")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES constraints_other_table(b)")
				testutils.AssertQueryRuns(connection, "ALTER TABLE ONLY constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")
				oid := testutils.OidFromRelationName(connection, "constraints_table")

				constraints := backup.GetConstraints(connection, oid)

				Expect(len(constraints)).To(Equal(4))
				Expect(constraints[0]).To(Equal(uniqueConstraint))
				Expect(constraints[1]).To(Equal(pkConstraint))
				Expect(constraints[2]).To(Equal(fkConstraint))
				Expect(constraints[3]).To(Equal(checkConstraint))
			})
		})
	})
	Describe("GetDistributionPolicy", func() {
		It("returns distribution policy info for a table DISTRIBUTED RANDOMLY", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_random(a int, b text) DISTRIBUTED RANDOMLY")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_random")
			oid := testutils.OidFromRelationName(connection, "dist_random")

			distPolicy := backup.GetDistributionPolicy(connection, oid)

			Expect(distPolicy).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY one column", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_one(a int, b text) DISTRIBUTED BY (a)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_one")
			oid := testutils.OidFromRelationName(connection, "dist_one")

			distPolicy := backup.GetDistributionPolicy(connection, oid)

			Expect(distPolicy).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY two columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_two(a int, b text) DISTRIBUTED BY (a, b)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_two")
			oid := testutils.OidFromRelationName(connection, "dist_two")

			distPolicy := backup.GetDistributionPolicy(connection, oid)

			Expect(distPolicy).To(Equal("DISTRIBUTED BY (a, b)"))
		})
	})
	Describe("GetAllSequences", func() {
		It("", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.my_sequence IS 'this is a sequence comment'")

			testutils.AssertQueryRuns(connection, "CREATE SCHEMA test_schema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE test_schema.my_sequence2")

			sequences := backup.GetAllSequences(connection)

			Expect(len(sequences)).To(Equal(2))
			Expect(sequences[0].SchemaOid).ToNot(Equal(0))
			Expect(sequences[0].RelationOid).ToNot(Equal(0))
			Expect(sequences[0].SchemaName).To(Equal("public"))
			Expect(sequences[0].RelationName).To(Equal("my_sequence"))
			Expect(sequences[0].Comment).To(Equal("this is a sequence comment"))
			Expect(sequences[0].Owner).To(Equal("testrole"))

			Expect(sequences[1].SchemaOid).ToNot(Equal(0))
			Expect(sequences[1].RelationOid).ToNot(Equal(0))
			Expect(sequences[1].SchemaName).To(Equal("test_schema"))
			Expect(sequences[1].RelationName).To(Equal("my_sequence2"))
			Expect(sequences[1].Comment).To(Equal(""))
			Expect(sequences[1].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetSequenceDefinition", func() {
		It("returns sequence information for sequence with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			sequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			Expect(sequenceDef.Name).To(Equal("my_sequence"))
			Expect(sequenceDef.LastVal).To(Equal(int64(1)))
			Expect(sequenceDef.Increment).To(Equal(int64(1)))
			Expect(sequenceDef.MaxVal).To(Equal(int64(9223372036854775807)))
			Expect(sequenceDef.MinVal).To(Equal(int64(1)))
			Expect(sequenceDef.CacheVal).To(Equal(int64(1)))
			Expect(sequenceDef.LogCnt).To(Equal(int64(0)))
			Expect(sequenceDef.IsCycled).To(Equal(false))
			Expect(sequenceDef.IsCalled).To(Equal(false))
		})
		It("returns sequence information for a complex sequence", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_sequence(a int, b char(20))")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_sequence")
			testutils.AssertQueryRuns(connection,
				"CREATE SEQUENCE my_sequence INCREMENT BY 5 MINVALUE 20 MAXVALUE 1000 START 100 OWNED BY with_sequence.a")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'acme')")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'beta')")

			sequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			Expect(sequenceDef.Name).To(Equal("my_sequence"))
			Expect(sequenceDef.LastVal).To(Equal(int64(105)))
			Expect(sequenceDef.Increment).To(Equal(int64(5)))
			Expect(sequenceDef.MaxVal).To(Equal(int64(1000)))
			Expect(sequenceDef.MinVal).To(Equal(int64(20)))
			Expect(sequenceDef.CacheVal).To(Equal(int64(1)))
			Expect(sequenceDef.LogCnt).To(Equal(int64(31)))
			Expect(sequenceDef.IsCycled).To(Equal(false))
			Expect(sequenceDef.IsCalled).To(Equal(true))
		})
	})
	Describe("GetSequenceOwnerMap", func() {
		It("returns sequence information for sequences owned by columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE without_sequence(a int, b char(20));")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE without_sequence")
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_sequence(a int, b char(20));")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_sequence")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence OWNED BY with_sequence.a;")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			sequenceMap := backup.GetSequenceOwnerMap(connection)

			Expect(len(sequenceMap)).To(Equal(1))
			Expect(sequenceMap["public.my_sequence"]).To(Equal("with_sequence.a"))
		})
	})
	Describe("GetDistributionPolicy", func() {
		It("returns a slice for a table DISTRIBUTED RANDOMLY", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_random_dist(a int, b char(20)) DISTRIBUTED RANDOMLY")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_random_dist")
			oid := testutils.OidFromRelationName(connection, "with_random_dist")

			result := backup.GetDistributionPolicy(connection, oid)

			Expect(result).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns a slice for a table DISTRIBUTED BY one column", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_single_dist(a int, b char(20)) DISTRIBUTED BY (a)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_single_dist")
			oid := testutils.OidFromRelationName(connection, "with_single_dist")

			result := backup.GetDistributionPolicy(connection, oid)

			Expect(result).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns a slice for a table DISTRIBUTED BY two columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_multiple_dist(a int, b char(20)) DISTRIBUTED BY (a, b)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_multiple_dist")
			oid := testutils.OidFromRelationName(connection, "with_multiple_dist")

			result := backup.GetDistributionPolicy(connection, oid)

			Expect(result).To(Equal("DISTRIBUTED BY (a, b)"))
		})
	})
	Describe("GetAllSequenceDefinitions", func() {
		It("returns a slice of definitions for all sequences", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_one START 3")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_one")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.seq_one IS 'this is a sequence comment'")

			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_two START 7")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_two")

			results := backup.GetAllSequenceDefinitions(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].SchemaName).To(Equal("public"))
			Expect(results[0].Name).To(Equal("seq_one"))
			Expect(results[0].LastVal).To(Equal(int64(3)))
			Expect(results[0].Increment).To(Equal(int64(1)))
			Expect(results[0].Comment).To(Equal("this is a sequence comment"))
			Expect(results[0].Owner).To(Equal("testrole"))
			Expect(results[1].SchemaName).To(Equal("public"))
			Expect(results[1].Name).To(Equal("seq_two"))
			Expect(results[1].LastVal).To(Equal(int64(7)))
			Expect(results[1].Increment).To(Equal(int64(1)))
			Expect(results[1].Comment).To(Equal(""))
			Expect(results[1].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetSequenceDefinition", func() {
		It("returns a slice for a sequence", func() {
			testutils.AssertQueryRuns(connection, `CREATE SEQUENCE mysequence
MAXVALUE 1000
CACHE 41
START 42
CYCLE`)
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE mysequence")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.mysequence IS 'this is a sequence comment'")

			result := backup.GetSequenceDefinition(connection, "mysequence")
			Expect(result.Name).To(Equal("mysequence"))
			Expect(result.LastVal).To(Equal(int64(42)))
			Expect(result.Increment).To(Equal(int64(1)))
			Expect(result.MaxVal).To(Equal(int64(1000)))
			Expect(result.MinVal).To(Equal(int64(1)))
			Expect(result.CacheVal).To(Equal(int64(41)))
			Expect(result.IsCycled).To(BeTrue())
			Expect(result.IsCalled).To(BeFalse())
		})
	})
	Describe("GetSessionGUCs", func() {
		It("returns a slice of values for session level GUCs", func() {
			/*
			 * We shouldn't need to run any setup queries, because we're using
			 * the default values for GPDB 5.
			 */
			results := backup.GetSessionGUCs(connection)
			Expect(results.ClientEncoding).To(Equal("UTF8"))
			Expect(results.StdConformingStrings).To(Equal("on"))
			Expect(results.DefaultWithOids).To(Equal("off"))
		})
	})
	Describe("GetIndexMetadata", func() {
		It("returns no slice when no index exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromRelationName(connection, "simple_table")

			results := backup.GetIndexMetadata(connection, oid)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx2 ON simple_table(j)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx2")
			testutils.AssertQueryRuns(connection, "COMMENT ON INDEX simple_table_idx2 IS 'this is a index comment'")
			oid := testutils.OidFromRelationName(connection, "simple_table")

			results := backup.GetIndexMetadata(connection, oid)

			Expect(len(results)).To(Equal(2))
			Expect(results[0].Name).To(Equal("simple_table_idx1"))
			Expect(results[0].Def).To(Equal("CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[1].Name).To(Equal("simple_table_idx2"))
			Expect(results[1].Def).To(Equal("CREATE INDEX simple_table_idx2 ON simple_table USING btree (j)"))
			Expect(results[1].Comment).To(Equal("this is a index comment"))
		})
	})
	Describe("GetDatabaseComment", func() {
		It("returns empty string for a database comment", func() {
			result := backup.GetDatabaseComment(connection)
			Expect(result).To(Equal(""))
		})
		It("returns a value for a database comment", func() {
			testutils.AssertQueryRuns(connection, "COMMENT ON DATABASE testdb IS 'this is a database comment'")
			defer testutils.AssertQueryRuns(connection, "COMMENT ON DATABASE testdb IS NULL")
			result := backup.GetDatabaseComment(connection)
			Expect(result).To(Equal("this is a database comment"))
		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			testutils.AssertQueryRuns(connection, "CREATE LANGUAGE plpythonu")
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plpythonu")
			handlerOid := testutils.OidFromFunctionName(connection, "plpython_call_handler")
			inlineOid := testutils.OidFromFunctionName(connection, "plpython_inline_handler")

			results := backup.GetProceduralLanguages(connection)

			Expect(len(results)).To(Equal(2))
			Expect(results[0].Name).To(Equal("plpgsql"))
			Expect(results[0].PlTrusted).To(BeTrue())
			Expect(results[1].Name).To(Equal("plpythonu"))
			Expect(results[1].Owner).To(Equal("testrole"))
			Expect(results[1].IsPl).To(BeTrue())
			Expect(results[1].PlTrusted).To(BeFalse())
			Expect(results[1].Handler).To(Equal(handlerOid))
			Expect(results[1].Inline).To(Equal(inlineOid))
			Expect(results[1].Validator).To(Equal(uint32(0)))
			Expect(results[1].Access).To(Equal(""))
			Expect(results[1].Comment).To(Equal(""))
		})
	})
	Describe("GetTypeDefinitions", func() {
		It("returns a slice of composite types", func() {
			testutils.AssertQueryRuns(connection, "create type comp_type as (name int4, name1 int, name2 text);")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(3))
			Expect(results[0].TypeSchema).To(Equal("public"))
			Expect(results[0].TypeName).To(Equal("comp_type"))
			Expect(results[0].Type).To(Equal("c"))
			Expect(results[0].AttName).To(Equal("name"))
			Expect(results[0].AttType).To(Equal("integer"))
			Expect(results[0].Input).To(Equal("record_in"))
			Expect(results[0].Output).To(Equal("record_out"))
			Expect(results[0].Receive).To(Equal("record_recv"))
			Expect(results[0].Send).To(Equal("record_send"))
			Expect(results[0].ModIn).To(Equal("-"))
			Expect(results[0].ModOut).To(Equal("-"))
			Expect(results[0].InternalLength).To(Equal(-1))
			Expect(results[0].IsPassedByValue).To(Equal(false))
			Expect(results[0].Alignment).To(Equal("d"))
			Expect(results[0].Storage).To(Equal("x"))
			Expect(results[0].DefaultVal).To(Equal(""))
			Expect(results[0].Element).To(Equal("-"))
			Expect(results[0].Delimiter).To(Equal(","))
			Expect(results[0].EnumLabels).To(Equal(""))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal("testrole"))

			Expect(results[1].TypeSchema).To(Equal("public"))
			Expect(results[1].TypeName).To(Equal("comp_type"))
			Expect(results[1].Type).To(Equal("c"))
			Expect(results[1].AttName).To(Equal("name1"))
			Expect(results[1].AttType).To(Equal("integer"))

			Expect(results[2].TypeSchema).To(Equal("public"))
			Expect(results[2].TypeName).To(Equal("comp_type"))
			Expect(results[2].Type).To(Equal("c"))
			Expect(results[2].AttName).To(Equal("name2"))
			Expect(results[2].AttType).To(Equal("text"))
		})
		It("returns a slice for a base type with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS opaque AS 'boolin' LANGUAGE internal")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION base_fn_in(cstring)")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(opaque) RETURNS opaque AS 'boolout' LANGUAGE internal")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION base_fn_out(base_type) CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0].TypeSchema).To(Equal("public"))
			Expect(results[0].TypeName).To(Equal("base_type"))
			Expect(results[0].Type).To(Equal("b"))
			Expect(results[0].AttName).To(Equal(""))
			Expect(results[0].AttType).To(Equal(""))
			Expect(results[0].Input).To(Equal("base_fn_in"))
			Expect(results[0].Output).To(Equal("base_fn_out"))
			Expect(results[0].Receive).To(Equal("-"))
			Expect(results[0].Send).To(Equal("-"))
			Expect(results[0].ModIn).To(Equal("-"))
			Expect(results[0].ModOut).To(Equal("-"))
			Expect(results[0].InternalLength).To(Equal(-1))
			Expect(results[0].IsPassedByValue).To(Equal(false))
			Expect(results[0].Alignment).To(Equal("i"))
			Expect(results[0].Storage).To(Equal("p"))
			Expect(results[0].DefaultVal).To(Equal(""))
			Expect(results[0].Element).To(Equal("-"))
			Expect(results[0].Delimiter).To(Equal(","))
			Expect(results[0].EnumLabels).To(Equal(""))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal("testrole"))
		})
		It("returns a slice for a base type with custom configuration", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS opaque AS 'boolin' LANGUAGE internal")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION base_fn_in(cstring)")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(opaque) RETURNS opaque AS 'boolout' LANGUAGE internal")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION base_fn_out(base_type) CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=char, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")
			testutils.AssertQueryRuns(connection, "COMMENT ON TYPE base_type IS 'this is a type comment'")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0].TypeSchema).To(Equal("public"))
			Expect(results[0].TypeName).To(Equal("base_type"))
			Expect(results[0].Type).To(Equal("b"))
			Expect(results[0].AttName).To(Equal(""))
			Expect(results[0].AttType).To(Equal(""))
			Expect(results[0].Input).To(Equal("base_fn_in"))
			Expect(results[0].Output).To(Equal("base_fn_out"))
			Expect(results[0].Receive).To(Equal("-"))
			Expect(results[0].Send).To(Equal("-"))
			Expect(results[0].ModIn).To(Equal("-"))
			Expect(results[0].ModOut).To(Equal("-"))
			Expect(results[0].InternalLength).To(Equal(8))
			Expect(results[0].IsPassedByValue).To(Equal(true))
			Expect(results[0].Alignment).To(Equal("c"))
			Expect(results[0].Storage).To(Equal("p"))
			Expect(results[0].DefaultVal).To(Equal("0"))
			Expect(results[0].Element).To(Equal("integer"))
			Expect(results[0].Delimiter).To(Equal(";"))
			Expect(results[0].EnumLabels).To(Equal(""))
			Expect(results[0].Comment).To(Equal("this is a type comment"))
			Expect(results[0].Owner).To(Equal("testrole"))
		})
		It("returns a slice for an enum type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE enum_type AS ENUM ('label1','label2','label3')")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			results := backup.GetTypeDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0].TypeSchema).To(Equal("public"))
			Expect(results[0].TypeName).To(Equal("enum_type"))
			Expect(results[0].Type).To(Equal("e"))
			Expect(results[0].AttName).To(Equal(""))
			Expect(results[0].AttType).To(Equal(""))
			Expect(results[0].Input).To(Equal("enum_in"))
			Expect(results[0].Output).To(Equal("enum_out"))
			Expect(results[0].Receive).To(Equal("enum_recv"))
			Expect(results[0].Send).To(Equal("enum_send"))
			Expect(results[0].ModIn).To(Equal("-"))
			Expect(results[0].ModOut).To(Equal("-"))
			Expect(results[0].InternalLength).To(Equal(4))
			Expect(results[0].IsPassedByValue).To(Equal(true))
			Expect(results[0].Alignment).To(Equal("i"))
			Expect(results[0].Storage).To(Equal("p"))
			Expect(results[0].DefaultVal).To(Equal(""))
			Expect(results[0].Element).To(Equal("-"))
			Expect(results[0].Delimiter).To(Equal(","))
			Expect(results[0].EnumLabels).To(Equal("'label1',\n\t'label2',\n\t'label3'"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal("testrole"))
		})
		// TODO: Add integration test combining all types once catalog issue is fixed
	})
	Describe("GetExternalTablesMap", func() {
		It("returns empty map", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			result := backup.GetExternalTablesMap(connection)

			Expect(len(result)).To(Equal(0))
		})
		It("returns map with external tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")

			result := backup.GetExternalTablesMap(connection)

			Expect(len(result)).To(Equal(1))
			Expect(result["public.ext_table"]).To(BeTrue())
		})
		// TODO: Add tests for external partitions
	})
	Describe("GetExternalTableDefinition", func() {
		It("returns a slice for a basic external table definition", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")
			oid := testutils.OidFromRelationName(connection, "ext_table")

			result := backup.GetExternalTableDefinition(connection, oid)

			Expect(result.Type).To(Equal(0))
			Expect(result.Protocol).To(Equal(0))
			Expect(result.Location).To(Equal("file://tmp/myfile.txt"))
			Expect(result.ExecLocation).To(Equal("ALL_SEGMENTS"))
			Expect(result.FormatType).To(Equal("t"))
			Expect(result.FormatOpts).To(Equal("delimiter '	' null '\\N' escape '\\'"))
			Expect(result.Options).To(Equal(""))
			Expect(result.Command).To(Equal(""))
			Expect(result.RejectLimit).To(Equal(0))
			Expect(result.RejectLimitType).To(Equal(""))
			Expect(result.ErrTable).To(Equal(""))
			Expect(result.Encoding).To(Equal("UTF8"))
			Expect(result.Writable).To(BeFalse())
		})
		It("returns a slice for a complex external table definition", func() {
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'
OPTIONS (foo 'bar')
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")
			oid := testutils.OidFromRelationName(connection, "ext_table")

			result := backup.GetExternalTableDefinition(connection, oid)

			Expect(result.Type).To(Equal(0))
			Expect(result.Protocol).To(Equal(0))
			Expect(result.Location).To(Equal("file://tmp/myfile.txt"))
			Expect(result.ExecLocation).To(Equal("ALL_SEGMENTS"))
			Expect(result.FormatType).To(Equal("t"))
			Expect(result.FormatOpts).To(Equal("delimiter '	' null '\\N' escape '\\'"))
			Expect(result.Options).To(Equal("foo 'bar'"))
			Expect(result.Command).To(Equal(""))
			Expect(result.RejectLimit).To(Equal(10))
			Expect(result.RejectLimitType).To(Equal("p"))
			Expect(result.ErrTable).To(Equal("ext_table"))
			Expect(result.Encoding).To(Equal("UTF8"))
			Expect(result.Writable).To(BeFalse())
		})
		// TODO: Add tests for external partitions
	})
	Describe("GetDatabaseGUCs", func() {
		It("returns a slice of values for database level GUCs", func() {
			testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET default_with_oids TO true")
			defer testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET default_with_oids TO false")
			testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET search_path TO public,pg_catalog")
			defer testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET search_path TO pg_catalog,public")
			results := backup.GetDatabaseGUCs(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("SET default_with_oids TO true"))
			Expect(results[1]).To(Equal("SET search_path TO public, pg_catalog"))
		})
	})
	Describe("GetDatabaseOwner", func() {
		It("returns a value for database owner", func() {
			result := backup.GetDatabaseOwner(connection)
			Expect(result).To(Equal("gpadmin"))
		})
	})
	Describe("GetPartitionDefinition", func() {
		It("returns empty string when no partition exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromRelationName(connection, "simple_table")

			result := backup.GetPartitionDefinition(connection, oid)

			Expect(result).To(Equal(""))
		})
		It("returns a value for a partition defintiion", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part_table (id int, rank int, year int, gender 
char(1), count int ) 
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'), 
  PARTITION boys VALUES ('M'), 
  DEFAULT PARTITION other );
			`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part_table")
			oid := testutils.OidFromRelationName(connection, "part_table")

			result := backup.GetPartitionDefinition(connection, oid)

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `PARTITION BY LIST(gender) 
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=false ), 
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=false ), 
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=false )
          )`
			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetPartitionDefinitionTemplate", func() {
		It("returns empty string when no partition definition template exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromRelationName(connection, "simple_table")

			result := backup.GetPartitionTemplateDefinition(connection, oid)

			Expect(result).To(Equal(""))
		})
		It("returns a value for a subpartition template", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part_table")
			oid := testutils.OidFromRelationName(connection, "part_table")

			result := backup.GetPartitionTemplateDefinition(connection, oid)

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `ALTER TABLE part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`

			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetStorageOptions", func() {
		It("returns an empty string when no table storage options exist ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromRelationName(connection, "simple_table")

			result := backup.GetStorageOptions(connection, oid)

			Expect(result).To(Equal(""))
		})
		It("returns a value for storage options of a table ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE ao_table(i int) with (appendonly=true)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE ao_table")
			oid := testutils.OidFromRelationName(connection, "ao_table")

			result := backup.GetStorageOptions(connection, oid)

			Expect(result).To(Equal("appendonly=true"))
		})
	})
	Describe("GetFunctionDefinitions", func() {
		It("returns a slice of function definitions", func() {
			testutils.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")
			testutils.AssertQueryRuns(connection, "COMMENT ON FUNCTION append(integer, integer) IS 'this is a function comment'")

			results := backup.GetFunctionDefinitions(connection)

			Expect(len(results)).To(Equal(2))
			Expect(results[0].SchemaName).To(Equal("public"))
			Expect(results[0].FunctionName).To(Equal("add"))
			Expect(results[0].ReturnsSet).To(BeFalse())
			Expect(results[0].FunctionBody).To(Equal("SELECT $1 + $2"))
			Expect(results[0].BinaryPath).To(Equal(""))
			Expect(results[0].Arguments).To(Equal("integer, integer"))
			Expect(results[0].IdentArgs).To(Equal("integer, integer"))
			Expect(results[0].ResultType).To(Equal("integer"))
			Expect(results[0].Volatility).To(Equal("v"))
			Expect(results[0].IsStrict).To(BeFalse())
			Expect(results[0].IsSecurityDefiner).To(BeFalse())
			Expect(results[0].Config).To(Equal(""))
			Expect(results[0].Cost).To(Equal(float32(100)))
			Expect(results[0].NumRows).To(Equal(float32(0)))
			Expect(results[0].SqlUsage).To(Equal("c"))
			Expect(results[0].Language).To(Equal("sql"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal("testrole"))

			Expect(results[1].SchemaName).To(Equal("public"))
			Expect(results[1].FunctionName).To(Equal("append"))
			Expect(results[1].ReturnsSet).To(BeTrue())
			Expect(results[1].FunctionBody).To(Equal("SELECT ($1, $2)"))
			Expect(results[1].BinaryPath).To(Equal(""))
			Expect(results[1].Arguments).To(Equal("integer, integer"))
			Expect(results[1].IdentArgs).To(Equal("integer, integer"))
			Expect(results[1].ResultType).To(Equal("SETOF record"))
			Expect(results[1].Volatility).To(Equal("s"))
			Expect(results[1].IsStrict).To(BeTrue())
			Expect(results[1].IsSecurityDefiner).To(BeTrue())
			Expect(results[1].Config).To(Equal("SET search_path TO pg_temp"))
			Expect(results[1].Cost).To(Equal(float32(200)))
			Expect(results[1].NumRows).To(Equal(float32(200)))
			Expect(results[1].SqlUsage).To(Equal("m"))
			Expect(results[1].Language).To(Equal("sql"))
			Expect(results[1].Comment).To(Equal("this is a function comment"))
			Expect(results[1].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetAggregateDefinitions", func() {
		It("returns a slice of aggregate definitions", func() {
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
CREATE FUNCTION mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
CREATE AGGREGATE agg_prefunc(numeric, numeric) (
	SFUNC = mysfunc_accum,
	STYPE = numeric,
	PREFUNC = mypre_accum,
	INITCOND = 0 );
`)
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromFunctionName(connection, "mysfunc_accum")
			prelimOid := testutils.OidFromFunctionName(connection, "mypre_accum")
			finalOid := uint32(0)
			sortOid := uint32(0)

			result := backup.GetAggregateDefinitions(connection)

			Expect(len(result)).To(Equal(1))
			Expect(result[0].SchemaName).To(Equal("public"))
			Expect(result[0].AggregateName).To(Equal("agg_prefunc"))
			Expect(result[0].Arguments).To(Equal("numeric, numeric"))
			Expect(result[0].IdentArgs).To(Equal("numeric, numeric"))
			Expect(result[0].TransitionFunction).To(Equal(transitionOid))
			Expect(result[0].PreliminaryFunction).To(Equal(prelimOid))
			Expect(result[0].FinalFunction).To(Equal(finalOid))
			Expect(result[0].SortOperator).To(Equal(sortOid))
			Expect(result[0].TransitionDataType).To(Equal("numeric"))
			Expect(result[0].InitialValue).To(Equal("0"))
			Expect(result[0].IsOrdered).To(BeFalse())
			Expect(result[0].Comment).To(Equal(""))
			Expect(result[0].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetFunctionOidToInfoMap", func() {
		It("returns map containing function information", func() {
			result := backup.GetFunctionOidToInfoMap(connection)
			initialLength := len(result)
			testutils.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			result = backup.GetFunctionOidToInfoMap(connection)
			oid := testutils.OidFromFunctionName(connection, "add")
			Expect(len(result)).To(Equal(initialLength + 1))
			Expect(result[oid].QualifiedName).To(Equal("public.add"))
			Expect(result[oid].Arguments).To(Equal("integer, integer"))
		})
		It("returns map with external tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, `CREATE READABLE EXTERNAL TABLE ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT' ( DELIMITER '|' NULL ' ')`)
			defer testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE ext_table")

			result := backup.GetExternalTablesMap(connection)

			Expect(len(result)).To(Equal(1))
			Expect(result["public.ext_table"]).To(BeTrue())
		})
	})
	Describe("GetCastDefinitions", func() {
		It("returns a slice for a basic cast", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")
			testutils.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION casttoint(text) AS ASSIGNMENT")
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")

			results := backup.GetCastDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0].SourceType).To(Equal("text"))
			Expect(results[0].TargetType).To(Equal("integer"))
			Expect(results[0].FunctionSchema).To(Equal("public"))
			Expect(results[0].FunctionName).To(Equal("casttoint"))
			Expect(results[0].FunctionArgs).To(Equal("text"))
			Expect(results[0].CastContext).To(Equal("a"))
			Expect(results[0].Comment).To(Equal(""))
		})
		It("returns a slice for a basic cast with comment", func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")
			testutils.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION casttoint(text) AS ASSIGNMENT")
			defer testutils.AssertQueryRuns(connection, "DROP CAST (text AS integer)")
			testutils.AssertQueryRuns(connection, "COMMENT ON CAST (text AS integer) IS 'this is a cast comment'")

			results := backup.GetCastDefinitions(connection)

			Expect(len(results)).To(Equal(1))
			Expect(results[0].SourceType).To(Equal("text"))
			Expect(results[0].TargetType).To(Equal("integer"))
			Expect(results[0].FunctionSchema).To(Equal("public"))
			Expect(results[0].FunctionName).To(Equal("casttoint"))
			Expect(results[0].FunctionArgs).To(Equal("text"))
			Expect(results[0].CastContext).To(Equal("a"))
			Expect(results[0].Comment).To(Equal("this is a cast comment"))
		})
	})
})

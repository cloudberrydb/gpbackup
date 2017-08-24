package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetAllUserTables", func() {
		It("returns user table information for basic heap tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(t text)")

			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("public", "foo")
			tableTestTable := backup.BasicRelation("testschema", "testtable")

			Expect(len(tables)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatchExcluding(&tableTestTable, &tables[1], "SchemaOid", "RelationOid")
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

			tableRank := backup.BasicRelation("public", "rank")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableRank, &tables[0], "SchemaOid", "RelationOid")
		})
		It("returns user table information for table in specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetSchemaInclude([]string{"testschema"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.BasicRelation("testschema", "foo")

			Expect(len(tables)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "RelationOid")
		})
	})
	Describe("GetColumnDefinitions", func() {
		It("returns table attribute information for a heap table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE atttable(a float, b text, c text NOT NULL, d int DEFAULT(5), e text)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE atttable")
			testutils.AssertQueryRuns(connection, "COMMENT ON COLUMN atttable.a IS 'att comment'")
			testutils.AssertQueryRuns(connection, "ALTER TABLE atttable DROP COLUMN b")
			testutils.AssertQueryRuns(connection, "ALTER TABLE atttable ALTER COLUMN e SET STORAGE PLAIN")
			oid := testutils.OidFromObjectName(connection, "public", "atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, IsDropped: false, TypeName: "double precision", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "att comment"}
			columnC := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "c", NotNull: true, HasDefault: false, IsDropped: false, TypeName: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			columnD := backup.ColumnDefinition{Oid: 0, Num: 4, Name: "d", NotNull: false, HasDefault: true, IsDropped: false, TypeName: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "5", Comment: ""}
			columnE := backup.ColumnDefinition{Oid: 0, Num: 5, Name: "e", NotNull: false, HasDefault: false, IsDropped: false, TypeName: "text", Encoding: "", StatTarget: -1, StorageType: "PLAIN", DefaultVal: "", Comment: ""}

			Expect(len(tableAtts)).To(Equal(4))

			testutils.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnC, &tableAtts[1], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnD, &tableAtts[2], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnE, &tableAtts[3], "Oid")
		})
		It("returns table attributes including encoding for a column oriented table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE co_atttable(a float, b text ENCODING(blocksize=65536)) WITH (appendonly=true, orientation=column)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE co_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "co_atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, IsDropped: false, TypeName: "double precision", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			columnB := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "b", NotNull: false, HasDefault: false, IsDropped: false, TypeName: "text", Encoding: "blocksize=65536,compresstype=none,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}

			Expect(len(tableAtts)).To(Equal(2))

			testutils.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&columnB, &tableAtts[1], "Oid")
		})
		It("returns an empty attribute array for a table with no columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE nocol_atttable()")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE nocol_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "nocol_atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connection)[oid]

			Expect(len(tableAtts)).To(Equal(0))
		})
	})
	Describe("GetDistributionPolicies", func() {
		It("returns distribution policy info for a table DISTRIBUTED RANDOMLY", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_random(a int, b text) DISTRIBUTED RANDOMLY")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_random")
			oid := testutils.OidFromObjectName(connection, "public", "dist_random", backup.TYPE_RELATION)

			tables := []backup.Relation{{RelationOid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY one column", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_one(a int, b text) DISTRIBUTED BY (a)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			tables := []backup.Relation{{RelationOid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY two columns", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE dist_two(a int, b text) DISTRIBUTED BY (a, b)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE dist_two")
			oid := testutils.OidFromObjectName(connection, "public", "dist_two", backup.TYPE_RELATION)

			tables := []backup.Relation{{RelationOid: oid}}
			distPolicies := backup.GetDistributionPolicies(connection, tables)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a, b)"))
		})
	})
	Describe("GetPartitionDefinitions", func() {
		It("returns empty string when no partition exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for a partition definition", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part_table (id int, rank int, year int, gender 
char(1), count int ) 
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'), 
  PARTITION boys VALUES ('M'), 
  DEFAULT PARTITION other );
			`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part_table")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

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
	Describe("GetPartitionTemplates", func() {
		It("returns empty string when no partition definition template exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

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
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

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
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetStorageOptions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for storage options of a table ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE ao_table(i int) with (appendonly=true)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE ao_table")
			oid := testutils.OidFromObjectName(connection, "public", "ao_table", backup.TYPE_RELATION)

			result := backup.GetStorageOptions(connection)[oid]

			Expect(result).To(Equal("appendonly=true"))
		})
	})
	Describe("GetAllSequenceRelations", func() {
		It("returns a slice of all sequences", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.my_sequence IS 'this is a sequence comment'")

			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence2")

			sequences := backup.GetAllSequenceRelations(connection)

			mySequence := backup.BasicRelation("public", "my_sequence")
			mySequence2 := backup.BasicRelation("testschema", "my_sequence2")

			Expect(len(sequences)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatchExcluding(&mySequence2, &sequences[1], "SchemaOid", "RelationOid")
		})
		It("returns a slice of all sequences in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence")
			mySequence := backup.BasicRelation("testschema", "my_sequence")

			backup.SetSchemaInclude([]string{"testschema"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "RelationOid")
		})
	})
	Describe("GetSequenceDefinition", func() {
		It("returns sequence information for sequence with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}

			testutils.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
		It("returns sequence information for a complex sequence", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE with_sequence(a int, b char(20))")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE with_sequence")
			testutils.AssertQueryRuns(connection,
				"CREATE SEQUENCE my_sequence INCREMENT BY 5 MINVALUE 20 MAXVALUE 1000 START 100 OWNED BY with_sequence.a")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'acme')")
			testutils.AssertQueryRuns(connection, "INSERT INTO with_sequence VALUES (nextval('my_sequence'), 'beta')")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, LogCnt: 31, IsCycled: false, IsCalled: true}

			testutils.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
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

			sequenceMap := backup.GetSequenceColumnOwnerMap(connection)

			Expect(len(sequenceMap)).To(Equal(1))
			Expect(sequenceMap["public.my_sequence"]).To(Equal("public.with_sequence.a"))
		})
	})
	Describe("GetAllSequences", func() {
		It("returns a slice of definitions for all sequences", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_one START 3")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_one")
			testutils.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.seq_one IS 'this is a sequence comment'")

			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE seq_two START 7")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE seq_two")

			seqOneRelation := backup.BasicRelation("public", "seq_one")
			seqOneDef := backup.SequenceDefinition{Name: "seq_one", LastVal: 3, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			seqTwoRelation := backup.BasicRelation("public", "seq_two")
			seqTwoDef := backup.SequenceDefinition{Name: "seq_two", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}

			results := backup.GetAllSequences(connection)

			testutils.ExpectStructsToMatchExcluding(&seqOneRelation, &results[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatchExcluding(&seqOneDef, &results[0].SequenceDefinition)
			testutils.ExpectStructsToMatchExcluding(&seqTwoRelation, &results[1].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatchExcluding(&seqTwoDef, &results[1].SequenceDefinition)
		})
	})
	Describe("GetViews", func() {
		It("returns a slice for a basic view", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, SchemaName: "public", ViewName: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
		It("returns a slice for view in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE VIEW testschema.simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW testschema.simpleview")
			backup.SetSchemaInclude([]string{"testschema"})

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, SchemaName: "testschema", ViewName: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
	})
	Describe("ConstructTableDependencies", func() {
		child := backup.BasicRelation("public", "child")
		childOne := backup.BasicRelation("public", "child_one")
		childTwo := backup.BasicRelation("public", "child_two")
		It("constructs dependencies correctly if there is one table dependent on one table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child")

			child.RelationOid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there are two tables dependent on one table", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_one() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_one")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child_two() INHERITS (parent)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child_two")

			childOne.RelationOid = testutils.OidFromObjectName(connection, "public", "child_one", backup.TYPE_RELATION)
			childTwo.RelationOid = testutils.OidFromObjectName(connection, "public", "child_two", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne, childTwo}

			tables = backup.ConstructTableDependencies(connection, tables)

			Expect(len(tables)).To(Equal(2))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[1].DependsUpon)).To(Equal(1))
			Expect(tables[1].DependsUpon[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there is one table dependent on two tables", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent_one(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent_one")
			testutils.AssertQueryRuns(connection, "CREATE TABLE parent_two(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE parent_two")
			testutils.AssertQueryRuns(connection, "CREATE TABLE child() INHERITS (parent_one, parent_two)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE child")

			child.RelationOid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(2))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent_one"))
			Expect(tables[0].DependsUpon[1]).To(Equal("public.parent_two"))
		})
		It("constructs dependencies correctly if there are no table dependencies", func() {
			tables := []backup.Relation{}
			tables = backup.ConstructTableDependencies(connection, tables)
			Expect(len(tables)).To(Equal(0))
		})
	})
	Describe("ConstructViewDependencies", func() {
		It("constructs dependencies correctly for a view that depends on two other views", func() {
			testutils.AssertQueryRuns(connection, "CREATE VIEW parent1 AS SELECT relname FROM pg_class")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW parent1")
			testutils.AssertQueryRuns(connection, "CREATE VIEW parent2 AS SELECT relname FROM pg_class")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW parent2")
			testutils.AssertQueryRuns(connection, "CREATE VIEW child AS (SELECT * FROM parent1 UNION SELECT * FROM parent2)")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW child")

			childView := backup.View{}
			childView.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			views := []backup.View{childView}

			views = backup.ConstructViewDependencies(connection, views)

			Expect(len(views)).To(Equal(1))
			Expect(len(views[0].DependsUpon)).To(Equal(2))
			Expect(views[0].DependsUpon[0]).To(Equal("public.parent1"))
			Expect(views[0].DependsUpon[1]).To(Equal("public.parent2"))
		})
	})
})

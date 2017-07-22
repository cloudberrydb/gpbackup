package integration

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
	})
	Describe("PrintRegularTableCreateStatement", func() {
		var (
			extTableEmpty backup.ExternalTableDefinition
			testTable     utils.Relation
			tableDef      backup.TableDefinition
			/*
			 * We need to construct partitionDef and partTemplateDef piecemeal like this,
			 * or go fmt will remove the trailing whitespace and prevent literal comparison.
			 */
			partitionDef = `PARTITION BY LIST(gender) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ), ` + `
          DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
          )`
			subpartitionDef = `PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_girls_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_girls_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_girls_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_girls_2_prt_other_regions', appendonly=false )
                  ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_boys_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_boys_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_boys_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_boys_2_prt_other_regions', appendonly=false )
                  ), ` + `
          DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='rank_1_prt_other_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='rank_1_prt_other_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='rank_1_prt_other_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='rank_1_prt_other_2_prt_other_regions', appendonly=false )
                  )
          )`
			partTemplateDef = `ALTER TABLE testtable ` + `
SET SUBPARTITION TEMPLATE  ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
		)
		BeforeEach(func() {
			extTableEmpty = backup.ExternalTableDefinition{-2, -2, "", "ALL_SEGMENTS", "t", "", "", "", 0, "", "", "UTF-8", false}
			testTable = utils.BasicRelation("public", "testtable")
			tableDef = backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ExtTableDef: extTableEmpty}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE public.testtable")
		})
		It("creates a table with no attributes", func() {
			tableDef.ColumnDefs = []backup.ColumnDefinition{}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a basic heap table", func() {
			rowOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "", "", ""}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a complex heap table", func() {
			rowOneDefault := backup.ColumnDefinition{1, "i", false, true, false, "integer", "", "", "42"}
			rowNotNullDefault := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "", "", "'bar'::text"}
			tableDef.DistPolicy = "DISTRIBUTED BY (i, j)"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOneDefault, rowNotNullDefault}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a basic append-optimized column-oriented table", func() {
			rowOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "compresstype=zlib,blocksize=32768,compresslevel=1", "", ""}
			rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "compresstype=zlib,blocksize=32768,compresslevel=1", "", ""}
			tableDef.StorageOpts = "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a one-level partition table", func() {
			rowOne := backup.ColumnDefinition{1, "region", false, false, false, "text", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "gender", false, false, false, "text", "", "", ""}
			tableDef.PartDef = partitionDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("creates a two-level partition table", func() {
			rowOne := backup.ColumnDefinition{1, "region", false, false, false, "text", "", "", ""}
			rowTwo := backup.ColumnDefinition{2, "gender", false, false, false, "text", "", "", ""}
			tableDef.PartDef = subpartitionDef
			tableDef.PartTemplateDef = partTemplateDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		var (
			extTableEmpty = backup.ExternalTableDefinition{-2, -2, "", "ALL_SEGMENTS", "t", "", "", "", 0, "", "", "UTF-8", false}
			testTable     = utils.BasicRelation("public", "testtable")
			tableRow      = backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "", ""}
			tableDef      = backup.TableDefinition{DistPolicy: "DISTRIBUTED BY (i)", ColumnDefs: []backup.ColumnDefinition{tableRow}, ExtTableDef: extTableEmpty}
			tableMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			tableMetadata = backup.ObjectMetadata{Privileges: []backup.ACL{}}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE testtable")
		})
		It("prints only owner for a table with no comment or column comments", func() {
			tableMetadata.Owner = "testrole"
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultMetadata := backup.GetMetadataForObjectType(connection, backup.RelationParams)
			resultTableMetadata := resultMetadata[testTable.RelationOid]
			testutils.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
		})
		It("prints table comment, table owner, and column comments for a table with all three", func() {
			tableMetadata.Owner = "testrole"
			tableMetadata.Comment = "This is a table comment."
			tableDef.ColumnDefs[0].Comment = "This is a column comment."
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			testTable.RelationOid = backup.OidFromObjectName(connection, "testtable", backup.RelationParams)
			resultTableDef := backup.ConstructDefinitionsForTable(connection, testTable, false)
			testutils.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ExtTableDef")
			resultMetadata := backup.GetMetadataForObjectType(connection, backup.RelationParams)
			resultTableMetadata := resultMetadata[testTable.RelationOid]
			testutils.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
		})
	})
	Describe("PrintCreateViewStatements", func() {
		It("creates a view with privileges and a comment (can't specify owner in GPDB5)", func() {
			viewDef := backup.QueryViewDefinition{1, "public", "simpleview", "SELECT pg_roles.rolname FROM pg_roles;"}
			viewMetadataMap := testutils.DefaultMetadataMap("VIEW", true, true, true)
			viewMetadata := viewMetadataMap[1]

			backup.PrintCreateViewStatements(buffer, []backup.QueryViewDefinition{viewDef}, viewMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			resultViews := backup.GetViewDefinitions(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.RelationParams)

			viewDef.Oid = backup.OidFromObjectName(connection, "simpleview", backup.RelationParams)
			Expect(len(resultViews)).To(Equal(1))
			resultMetadata := resultMetadataMap[viewDef.Oid]
			testutils.ExpectStructsToMatch(&viewDef, &resultViews[0])
			testutils.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		var (
			columnOwnerMap      map[string]string
			sequence            utils.Relation
			sequenceDef         backup.Sequence
			sequenceMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			sequence = utils.Relation{0, 1, "public", "my_sequence"}
			sequenceDef = backup.Sequence{Relation: sequence}
			columnOwnerMap = map[string]string{}
			sequenceMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic sequence", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a complex sequence", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, LogCnt: 0, IsCycled: false, IsCalled: true}
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a sequence owned by a table column", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence",
				LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			columnOwnerMap["public.my_sequence"] = "sequence_table.a"
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			//Create table that sequence can be owned by
			testutils.AssertQueryRuns(connection, "CREATE TABLE sequence_table(a int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE sequence_table")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
		})
		It("creates a sequence with privileges, owner, and comment", func() {
			sequenceDef.QuerySequenceDefinition = backup.QuerySequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 1}
			sequenceMetadata := backup.ObjectMetadata{[]backup.ACL{testutils.DefaultACLWithout("testrole", "SEQUENCE", "UPDATE")}, "testrole", "This is a sequence comment."}
			sequenceMetadataMap[1] = sequenceMetadata
			backup.PrintCreateSequenceStatements(buffer, []backup.Sequence{sequenceDef}, columnOwnerMap, sequenceMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")

			resultSequences := backup.GetAllSequences(connection)

			Expect(len(resultSequences)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.RelationParams)
			oid := backup.OidFromObjectName(connection, "my_sequence", backup.RelationParams)
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "RelationOid")
			testutils.ExpectStructsToMatch(&sequenceDef.QuerySequenceDefinition, &resultSequences[0].QuerySequenceDefinition)
			testutils.ExpectStructsToMatch(&sequenceMetadata, &resultMetadata)
		})
	})
})

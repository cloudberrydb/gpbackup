package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_relations tests", func() {
	var toc *utils.TOC
	var backupfile *utils.FileWithByteCount
	testTable := backup.BasicRelation("public", "tablename")

	distRandom := "DISTRIBUTED RANDOMLY"
	distSingle := "DISTRIBUTED BY (i)"
	distComposite := "DISTRIBUTED BY (i, j)"

	rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", TypeName: "integer", StatTarget: -1}
	rowTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", TypeName: "character varying(20)", StatTarget: -1}

	heapOpts := ""
	aoOpts := "appendonly=true"
	coOpts := "appendonly=true, orientation=column"
	heapFillOpts := "fillfactor=42"
	coManyOpts := "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"

	partDefEmpty := ""
	partTemplateDefEmpty := ""
	colDefsEmpty := []backup.ColumnDefinition{}
	extTableEmpty := backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTable: "", Encoding: "UTF-8", Writable: false, URIs: nil}

	partDef := `PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	)
`

	partTemplateDef := `ALTER TABLE tablename
SET SUBPARTITION TEMPLATE
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='tablename'),
          SUBPARTITION asia VALUES('asia') WITH (tablename='tablename'),
          SUBPARTITION europe VALUES('europe') WITH (tablename='tablename'),
          DEFAULT SUBPARTITION other_regions  WITH (tablename='tablename')
          )
`

	noMetadata := backup.ObjectMetadata{}

	BeforeEach(func() {
		toc = &utils.TOC{}
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("Relation.ToString", func() {
		It("remains unquoted if neither the schema nor the table name contains special characters", func() {
			testTable := backup.BasicRelation(`schemaname`, `tablename`)
			expected := `schemaname.tablename`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if the schema name contains special characters", func() {
			testTable := backup.BasicRelation(`schema,name`, `tablename`)
			expected := `"schema,name".tablename`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if the table name contains special characters", func() {
			testTable := backup.BasicRelation(`schemaname`, `table,name`)
			expected := `schemaname."table,name"`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if both the schema and the table name contain special characters", func() {
			testTable := backup.BasicRelation(`schema,name`, `table,name`)
			expected := `"schema,name"."table,name"`
			Expect(testTable.ToString()).To(Equal(expected))
		})
	})
	Describe("RelationFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname.tablename`
			newTable := backup.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schemaname`))
			Expect(newTable.RelationName).To(Equal(`tablename`))
		})
		It("can parse a string with a quoted schema", func() {
			testString := `"schema,name".tablename`
			newTable := backup.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schema,name`))
			Expect(newTable.RelationName).To(Equal(`tablename`))
		})
		It("can parse a string with a quoted table", func() {
			testString := `schemaname."table,name"`
			newTable := backup.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schemaname`))
			Expect(newTable.RelationName).To(Equal(`table,name`))
		})
		It("can parse a string with both schema and table quoted", func() {
			testString := `"schema,name"."table,name"`
			newTable := backup.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schema,name`))
			Expect(newTable.RelationName).To(Equal(`table,name`))
		})
		It("panics if given an invalid string", func() {
			testString := `schema.name.table.name`
			defer testutils.ShouldPanicWithMessage(`schema.name.table.name is not a valid fully-qualified table expression`)
			backup.RelationFromString(testString)
		})
	})
	Describe("PrintCreateTableStatement", func() {
		tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ColumnDefs: colDefsEmpty, ExtTableDef: extTableEmpty}
		It("calls PrintRegularTableCreateStatement for a regular table", func() {
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}

			tableDef.IsExternal = false
			backup.PrintCreateTableStatement(backupfile, toc, testTable, tableDef, tableMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "tablename", "TABLE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("calls PrintExternalTableCreateStatement for an external table", func() {
			tableDef.IsExternal = true
			backup.PrintCreateTableStatement(backupfile, toc, testTable, tableDef, noMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) 
FORMAT 'text'
ENCODING 'UTF-8';`)
		})
	})
	Describe("PrintRegularTableCreateStatement", func() {
		rowDropped := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", IsDropped: true, TypeName: "character varying(20)", StatTarget: -1}
		rowOneEncoding := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", TypeName: "integer", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1}
		rowTwoEncoding := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", TypeName: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1}
		rowNotNull := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, TypeName: "character varying(20)", StatTarget: -1}
		rowEncodingNotNull := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, TypeName: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1}
		rowOneDef := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", HasDefault: true, TypeName: "integer", StatTarget: -1, DefaultVal: "42"}
		rowTwoDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, TypeName: "character varying(20)", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowTwoEncodingDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, TypeName: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowNotNullDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, TypeName: "character varying(20)", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowEncodingNotNullDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, TypeName: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowStats := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", TypeName: "integer", StatTarget: 3}
		colStorageType := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", TypeName: "integer", StatTarget: -1, StorageType: "PLAIN"}

		Context("No special table attributes", func() {
			tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ExtTableDef: extTableEmpty}
			It("prints a CREATE TABLE block with one line", func() {
				col := []backup.ColumnDefinition{rowOne}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				tableDef.ColumnDefs = colDefsEmpty
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowDropped}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ExtTableDef: extTableEmpty}
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncoding}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STATISTICS statement", func() {
				col := []backup.ColumnDefinition{rowStats}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STATISTICS 3;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STORAGE statement", func() {
				col := []backup.ColumnDefinition{colStorageType}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STORAGE PLAIN;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ExtTableDef: extTableEmpty}
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNull}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncodingDef}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNullDef}
				tableDef.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and storage options)", func() {
			col := []backup.ColumnDefinition{rowOne, rowTwo}
			It("has a single-column distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distSingle, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distComposite, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: aoOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distSingle, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: aoOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distComposite, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: aoOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distSingle, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distComposite, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapFillOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distSingle, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapFillOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distComposite, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapFillOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coManyOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distSingle, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coManyOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				tableDef := backup.TableDefinition{DistPolicy: distComposite, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coManyOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
			})
		})
		Context("Table partitioning", func() {
			col := []backup.ColumnDefinition{rowOne, rowTwo}
			It("is a partition table with table attributes", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDef, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);`)
			})
			It("is a partition table with no table attributes", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDef, PartTemplateDef: partTemplateDefEmpty, StorageOpts: coOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);`)
			})
			It("is a partition table with subpartitions and table attributes", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDef, PartTemplateDef: partTemplateDef, StorageOpts: heapOpts, ColumnDefs: col, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);
ALTER TABLE tablename
SET SUBPARTITION TEMPLATE
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='tablename'),
          SUBPARTITION asia VALUES('asia') WITH (tablename='tablename'),
          SUBPARTITION europe VALUES('europe') WITH (tablename='tablename'),
          DEFAULT SUBPARTITION other_regions  WITH (tablename='tablename')
          );`)
			})
		})
		Context("Tablespaces", func() {
			It("prints a CREATE TABLE block with a TABLESPACE clause", func() {
				tableDef := backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, TablespaceName: "test_tablespace", ColumnDefs: colDefsEmpty, ExtTableDef: extTableEmpty}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) TABLESPACE test_tablespace DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Inheritance", func() {
			tableDef := backup.TableDefinition{}
			BeforeEach(func() {
				tableDef = backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ExtTableDef: extTableEmpty}
			})
			AfterEach(func() {
				testTable.DependsUpon = []string{}
				testTable.Inherits = []string{}
			})
			It("prints a CREATE TABLE block with a single-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne}
				tableDef.ColumnDefs = col
				testTable.DependsUpon = []string{"public.parent"}
				testTable.Inherits = []string{"public.parent"}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) INHERITS (public.parent) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with a multiple-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef.ColumnDefs = col
				testTable.DependsUpon = []string{"public.parent_one", "public.parent_two"}
				testTable.Inherits = []string{"public.parent_one", "public.parent_two"}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) INHERITS (public.parent_one, public.parent_two) DISTRIBUTED RANDOMLY;`)
			})
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		testTable := backup.BasicRelation("public", "tablename")
		rowCommentOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", TypeName: "integer", StatTarget: -1, Comment: "This is a column comment."}
		rowCommentTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", TypeName: "integer", StatTarget: -1, Comment: "This is another column comment."}
		tableDef := backup.TableDefinition{}
		BeforeEach(func() {
			tableDef = backup.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, ExtTableDef: extTableEmpty}
		})

		It("prints a block with a table comment", func() {
			col := []backup.ColumnDefinition{rowOne}
			tableDef.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a single column comment", func() {
			col := []backup.ColumnDefinition{rowCommentOne}
			tableDef.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';`)
		})
		It("prints a block with multiple column comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			tableDef.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			col := []backup.ColumnDefinition{rowOne}
			tableDef.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			tableDef.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := backup.Relation{SchemaOid: 0, RelationOid: 1, SchemaName: "public", RelationName: "seq_name", DependsUpon: nil, Inherits: nil}
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqNegIncr := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -1, MinVal: -9223372036854775807, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 100, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 10, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -10, MinVal: -9223372036854775807, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -1, MinVal: -100, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqCycle := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: true, IsCalled: true}}
		seqStart := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: false}}
		emptySequenceMetadataMap := backup.MetadataMap{}

		It("can print a sequence with all default options", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "seq_name", "SEQUENCE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.Sequence{seqStart}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("can print a sequence with privileges, an owner, and a comment", func() {
			sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true)
			sequenceMetadata := sequenceMetadataMap[1]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[1] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';


ALTER TABLE public.seq_name OWNER TO testrole;


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;`)
		})
		It("can print a sequence with privileges WITH GRANT OPTION", func() {
			sequenceMetadataMap := backup.MetadataMap{
				1: {Privileges: []backup.ACL{testutils.DefaultACLWithGrantWithout("testrole", "SEQUENCE", "UPDATE")}}}
			sequenceMetadata := sequenceMetadataMap[1]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[1] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole WITH GRANT OPTION;`)
		})
	})
	Describe("PrintCreateViewStatements", func() {
		It("can print a basic view", func() {
			viewOne := backup.View{Oid: 0, SchemaName: "public", ViewName: "WowZa", Definition: "SELECT rolname FROM pg_role;", DependsUpon: []string{}}
			viewTwo := backup.View{Oid: 1, SchemaName: "shamwow", ViewName: "shazam", Definition: "SELECT count(*) FROM pg_tables;", DependsUpon: []string{}}
			viewMetadataMap := backup.MetadataMap{}
			backup.PrintCreateViewStatements(backupfile, toc, []backup.View{viewOne, viewTwo}, viewMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "WowZa", "VIEW")
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW public."WowZa" AS SELECT rolname FROM pg_role;`,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;`)
		})
		It("can print a view with privileges, an owner, and a comment", func() {
			viewOne := backup.View{Oid: 0, SchemaName: "public", ViewName: "WowZa", Definition: "SELECT rolname FROM pg_role;", DependsUpon: []string{}}
			viewTwo := backup.View{Oid: 1, SchemaName: "shamwow", ViewName: "shazam", Definition: "SELECT count(*) FROM pg_tables;", DependsUpon: []string{}}
			viewMetadataMap := testutils.DefaultMetadataMap("VIEW", true, true, true)
			backup.PrintCreateViewStatements(backupfile, toc, []backup.View{viewOne, viewTwo}, viewMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW public."WowZa" AS SELECT rolname FROM pg_role;`,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;


COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';


REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;`)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		baseSequence := backup.BasicRelation("public", "seq_name")
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 9223372036854775807, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		emptyColumnOwnerMap := make(map[string]string, 0)
		columnOwnerMap := map[string]string{"public.seq_name": "tablename.col_one"}
		It("prints nothing for a sequence without an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, emptyColumnOwnerMap)
			Expect(len(toc.PredataEntries)).To(Equal(0))
			testutils.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("can print an ALTER SEQUENCE statement for a sequence with an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, columnOwnerMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "seq_name", "SEQUENCE OWNER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER SEQUENCE public.seq_name OWNED BY tablename.col_one;`)
		})
	})
})

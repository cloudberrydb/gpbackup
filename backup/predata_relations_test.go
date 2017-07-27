package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_relations tests", func() {
	buffer := gbytes.NewBuffer()
	testTable := backup.BasicRelation("public", "tablename")

	distRandom := "DISTRIBUTED RANDOMLY"
	distSingle := "DISTRIBUTED BY (i)"
	distComposite := "DISTRIBUTED BY (i, j)"

	rowOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "", ""}
	rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "", "", ""}

	heapOpts := ""
	aoOpts := "appendonly=true"
	coOpts := "appendonly=true, orientation=column"
	heapFillOpts := "fillfactor=42"
	coManyOpts := "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"

	partDefEmpty := ""
	partTemplateDefEmpty := ""
	colDefsEmpty := []backup.ColumnDefinition{}
	extTableEmpty := backup.ExternalTableDefinition{-2, -2, "", "ALL_SEGMENTS", "t", "", "", "", 0, "", "", "UTF-8", false}

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

	Describe("PrintCreateTableStatement", func() {
		tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", colDefsEmpty, false, extTableEmpty}
		It("calls PrintRegularTableCreateStatement for a regular table", func() {
			tableDef.IsExternal = false
			backup.PrintCreateTableStatement(buffer, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
		})
		It("calls PrintExternalTableCreateStatement for an external table", func() {
			tableDef.IsExternal = true
			backup.PrintCreateTableStatement(buffer, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) 
FORMAT 'text'
ENCODING 'UTF-8';`)
		})
	})
	Describe("PrintRegularTableCreateStatement", func() {
		rowDropped := backup.ColumnDefinition{2, "j", false, false, true, "character varying(20)", "", "", ""}
		rowOneEncoding := backup.ColumnDefinition{1, "i", false, false, false, "integer", "compresstype=none,blocksize=32768,compresslevel=0", "", ""}
		rowTwoEncoding := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", ""}
		rowNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", "", "", ""}
		rowEncodingNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", ""}
		rowOneDef := backup.ColumnDefinition{1, "i", false, true, false, "integer", "", "", "42"}
		rowTwoDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", "", "", "'bar'::text"}
		rowTwoEncodingDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", "'bar'::text"}
		rowNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "", "", "'bar'::text"}
		rowEncodingNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", "'bar'::text"}

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE block with one line", func() {
				col := []backup.ColumnDefinition{rowOne}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", colDefsEmpty, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowDropped}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncoding}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNull}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncodingDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNullDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and storage options)", func() {
			It("has a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, aoOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, aoOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, aoOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, coOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, coOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, coOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapFillOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, heapFillOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, heapFillOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, coManyOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, coManyOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, coManyOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
			})
		})
		Context("Table partitioning", func() {
			It("is a partition table with table attributes", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
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
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDefEmpty, coOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
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
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDef, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
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
          );
`)
			})
		})
		Context("Tablespaces", func() {
			It("prints a CREATE TABLE block with a TABLESPACE clause", func() {
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "test_tablespace", colDefsEmpty, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
) TABLESPACE test_tablespace DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Inheritance", func() {
			AfterEach(func() {
				testTable.DependsUpon = []string{}
			})
			It("prints a CREATE TABLE block with a single-inheritance INHERITS clause", func() {
				testTable.DependsUpon = []string{"public.parent"}
				col := []backup.ColumnDefinition{rowOne}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer
) INHERITS (public.parent) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with a multiple-inheritance INHERITS clause", func() {
				testTable.DependsUpon = []string{"public.parent_one", "public.parent_two"}
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
				backup.PrintRegularTableCreateStatement(buffer, testTable, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) INHERITS (public.parent_one, public.parent_two) DISTRIBUTED RANDOMLY;`)
			})
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		testTable := backup.BasicRelation("public", "tablename")
		rowCommentOne := backup.ColumnDefinition{1, "i", false, false, false, "integer", "", "This is a column comment.", ""}
		rowCommentTwo := backup.ColumnDefinition{2, "j", false, false, false, "integer", "", "This is another column comment.", ""}

		It("prints a block with a table comment", func() {
			col := []backup.ColumnDefinition{rowOne}
			tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a single column comment", func() {
			col := []backup.ColumnDefinition{rowCommentOne}
			tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';`)
		})
		It("prints a block with multiple column comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, noMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			col := []backup.ColumnDefinition{rowOne}
			tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts, "", col, false, extTableEmpty}
			tableMetadata := backup.ObjectMetadata{Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(buffer, testTable, tableDef, tableMetadata)
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
	})
	Describe("ConsolidateColumnInfo", func() {
		attsOne := backup.QueryTableAttributes{1, "i", false, false, false, "integer", "", ""}
		attsTwo := backup.QueryTableAttributes{2, "j", false, false, false, "integer", "", ""}
		attsThree := backup.QueryTableAttributes{3, "k", false, false, false, "integer", "", ""}
		attsOneDef := backup.QueryTableAttributes{1, "i", false, true, false, "integer", "", ""}
		attsTwoDef := backup.QueryTableAttributes{2, "j", false, true, false, "integer", "", ""}
		attsThreeDef := backup.QueryTableAttributes{3, "k", false, true, false, "integer", "", ""}

		defaultsOne := backup.QueryTableDefault{1, "1"}
		defaultsTwo := backup.QueryTableDefault{2, "2"}
		defaultsThree := backup.QueryTableDefault{3, "3"}
		It("has no DEFAULT columns", func() {
			atts := []backup.QueryTableAttributes{attsOne, attsTwo, attsThree}
			defaults := []backup.QueryTableDefault{}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (i)", func() {
			atts := []backup.QueryTableAttributes{attsOneDef, attsTwo, attsThree}
			defaults := []backup.QueryTableDefault{defaultsOne}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (j)", func() {
			atts := []backup.QueryTableAttributes{attsOne, attsTwoDef, attsThree}
			defaults := []backup.QueryTableDefault{defaultsTwo}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (k)", func() {
			atts := []backup.QueryTableAttributes{attsOne, attsTwo, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and j)", func() {
			atts := []backup.QueryTableAttributes{attsOneDef, attsTwoDef, attsThree}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsTwo}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has two DEFAULT columns (j and k)", func() {
			atts := []backup.QueryTableAttributes{attsOne, attsTwoDef, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsTwo, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and k)", func() {
			atts := []backup.QueryTableAttributes{attsOneDef, attsTwo, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has all DEFAULT columns", func() {
			atts := []backup.QueryTableAttributes{attsOneDef, attsTwoDef, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsTwo, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := backup.Relation{0, 1, "public", "seq_name", []string{}}
		seqDefault := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqNegIncr := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -9223372036854775807, 5, 42, false, true}}
		seqMaxPos := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 100, 1, 5, 42, false, true}}
		seqMinPos := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 10, 5, 42, false, true}}
		seqMaxNeg := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -10, -9223372036854775807, 5, 42, false, true}}
		seqMinNeg := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -100, 5, 42, false, true}}
		seqCycle := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, true, true}}
		seqStart := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, false}}
		emptySequenceMetadataMap := backup.MetadataMap{}
		sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true)

		It("can print a sequence with all default options", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.Sequence{seqStart}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptySequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("can print a sequence with privileges, an owner, and a comment", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(buffer, sequences, sequenceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';


ALTER TABLE public.seq_name OWNER TO testrole;


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT ALL ON SEQUENCE public.seq_name TO testrole;`)
		})
	})
	Describe("PrintCreateViewStatements", func() {
		It("can print a basic view", func() {
			viewOne := backup.QueryViewDefinition{0, "public", "WowZa", "SELECT rolname FROM pg_role;", []string{}}
			viewTwo := backup.QueryViewDefinition{1, "shamwow", "shazam", "SELECT count(*) FROM pg_tables;", []string{}}
			viewMetadataMap := backup.MetadataMap{}
			backup.PrintCreateViewStatements(buffer, []backup.QueryViewDefinition{viewOne, viewTwo}, viewMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE VIEW public."WowZa" AS SELECT rolname FROM pg_role;


CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;`)
		})
		It("can print a view with privileges, an owner, and a comment", func() {
			viewOne := backup.QueryViewDefinition{0, "public", "WowZa", "SELECT rolname FROM pg_role;", []string{}}
			viewTwo := backup.QueryViewDefinition{1, "shamwow", "shazam", "SELECT count(*) FROM pg_tables;", []string{}}
			viewMetadataMap := testutils.DefaultMetadataMap("VIEW", true, true, true)
			backup.PrintCreateViewStatements(buffer, []backup.QueryViewDefinition{viewOne, viewTwo}, viewMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE VIEW public."WowZa" AS SELECT rolname FROM pg_role;


CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;


COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';


REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;`)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		baseSequence := backup.BasicRelation("public", "seq_name")
		seqDefault := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		emptyColumnOwnerMap := make(map[string]string, 0)
		columnOwnerMap := map[string]string{"public.seq_name": "tablename.col_one"}
		It("prints nothing for a sequence without an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(buffer, sequences, emptyColumnOwnerMap)
			testutils.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("can print an ALTER SEQUENCE statement for a sequence with an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(buffer, sequences, columnOwnerMap)
			testutils.ExpectRegexp(buffer, `ALTER SEQUENCE public.seq_name OWNED BY tablename.col_one;`)
		})
	})
})

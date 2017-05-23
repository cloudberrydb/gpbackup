package backup_test

import (
	"gpbackup/backup"
	"gpbackup/testutils"
	"gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata tests", func() {
	Describe("PrintCreateTableStatement", func() {
		buffer := gbytes.NewBuffer()
		testTable := utils.BasicRelation("public", "tablename")
		rowOne := backup.ColumnDefinition{1, "i", false, false, false, "int", "", "", ""}
		rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "", "", ""}
		rowDropped := backup.ColumnDefinition{2, "j", false, false, true, "character varying(20)", "", "", ""}
		rowOneEncoding := backup.ColumnDefinition{1, "i", false, false, false, "int", "compresstype=none,blocksize=32768,compresslevel=0", "", ""}
		rowTwoEncoding := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", ""}
		rowNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", "", "", ""}
		rowEncodingNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", ""}
		rowOneDef := backup.ColumnDefinition{1, "i", false, true, false, "int", "", "", "42"}
		rowTwoDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", "", "", "'bar'::text"}
		rowTwoEncodingDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", "'bar'::text"}
		rowNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "", "", "'bar'::text"}
		rowEncodingNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", "compresstype=zlib,blocksize=65536,compresslevel=1", "", "'bar'::text"}

		distRandom := "DISTRIBUTED RANDOMLY"
		distSingle := "DISTRIBUTED BY (i)"
		distComposite := "DISTRIBUTED BY (i, j)"

		partDefEmpty := ""
		partDef := `PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	)
`

		partTemplateDefEmpty := ""
		partTemplateDef := `ALTER TABLE tablename
SET SUBPARTITION TEMPLATE
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='tablename'),
          SUBPARTITION asia VALUES('asia') WITH (tablename='tablename'),
          SUBPARTITION europe VALUES('europe') WITH (tablename='tablename'),
          DEFAULT SUBPARTITION other_regions  WITH (tablename='tablename')
          )
`

		heapOpts := ""
		aoOpts := "appendonly=true"
		coOpts := "appendonly=true, orientation=column"
		heapFillOpts := "fillfactor=42"
		coManyOpts := "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE block with one line", func() {
				col := []backup.ColumnDefinition{rowOne}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				col := []backup.ColumnDefinition{}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowDropped}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncoding}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNull}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncodingDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNullDef}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and storage options)", func() {
			It("has a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, aoOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, aoOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, aoOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, coOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, coOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, coOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, coManyOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distSingle, partDefEmpty, partTemplateDefEmpty, coManyOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distComposite, partDefEmpty, partTemplateDefEmpty, coManyOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
			})
		})
		Context("Table partitioning", func() {
			It("is a partition table with table attributes", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
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
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDefEmpty, coOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
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
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
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
			It("is a partition table with subpartitions and no table attributes", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				tableDef := backup.TableDefinition{distRandom, partDef, partTemplateDef, coOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
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
		Context("Comments and owners", func() {
			tableWithComment := utils.Relation{0, 0, "public", "tablename", "This is a table comment.", ""}
			tableWithOwner := utils.Relation{0, 0, "public", "tablename", "", "testrole"}
			rowCommentOne := backup.ColumnDefinition{1, "i", false, false, false, "int", "", "This is a column comment.", ""}
			rowCommentTwo := backup.ColumnDefinition{2, "j", false, false, false, "int", "", "This is another column comment.", ""}

			It("prints a CREATE TABLE block with a table comment", func() {
				col := []backup.ColumnDefinition{rowOne}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, tableWithComment, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
			})
			It("prints a CREATE TABLE block with a single column comment", func() {
				col := []backup.ColumnDefinition{rowCommentOne}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int
) DISTRIBUTED RANDOMLY;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';`)
			})
			It("prints a CREATE TABLE block with multiple column comments", func() {
				col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, testTable, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int,
	j int
) DISTRIBUTED RANDOMLY;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
			})
			It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
				col := []backup.ColumnDefinition{rowOne, rowDropped}
				tableDef := backup.TableDefinition{distRandom, partDefEmpty, partTemplateDefEmpty, heapOpts}
				backup.PrintCreateTableStatement(buffer, tableWithOwner, col, tableDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE public.tablename (
	i int
) DISTRIBUTED RANDOMLY;


ALTER TABLE public.tablename OWNER TO testrole;`)
			})
		})
	})
	Describe("ProcessConstraints", func() {
		testTable := utils.BasicRelation("public", "tablename")
		uniqueOne := backup.QueryConstraint{"tablename_i_key", "u", "UNIQUE (i)", ""}
		uniqueTwo := backup.QueryConstraint{"tablename_j_key", "u", "UNIQUE (j)", ""}
		primarySingle := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i)", ""}
		primaryComposite := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i, j)", ""}
		foreignOne := backup.QueryConstraint{"tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)", ""}
		foreignTwo := backup.QueryConstraint{"tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)", ""}
		commentOne := backup.QueryConstraint{"tablename_i_key", "u", "UNIQUE (i)", "This is a constraint comment."}

		Context("No ALTER TABLE statements", func() {
			It("returns an empty slice", func() {
				constraints := []backup.QueryConstraint{}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(0))
			})
		})
		Context("ALTER TABLE statements involving different columns", func() {
			It("returns a slice containing one UNIQUE constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
			})
			It("returns a slice containing two UNIQUE constraints", func() {
				constraints := []backup.QueryConstraint{uniqueOne, uniqueTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(2))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(cons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
			It("returns a slice containing PRIMARY KEY constraint on one column", func() {
				constraints := []backup.QueryConstraint{primarySingle}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
			})
			It("returns a slice containing composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.QueryConstraint{primaryComposite}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
			})
			It("returns a slice containing one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(1))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing two FOREIGN KEY constraints", func() {
				constraints := []backup.QueryConstraint{foreignOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(2))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
				Expect(fkCons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint with a comment and one without", func() {
				constraints := []backup.QueryConstraint{commentOne, uniqueTwo}
				cons, _ := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(2))
				Expect(cons[0]).To(Equal(`

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);

COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';`))
				Expect(cons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
		})
		Context("ALTER TABLE statements involving the same column", func() {
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
		})
	})
	Describe("ConsolidateColumnInfo", func() {
		attsOne := backup.QueryTableAtts{1, "i", false, false, false, "int", "", ""}
		attsTwo := backup.QueryTableAtts{2, "j", false, false, false, "int", "", ""}
		attsThree := backup.QueryTableAtts{3, "k", false, false, false, "int", "", ""}
		attsOneDef := backup.QueryTableAtts{1, "i", false, true, false, "int", "", ""}
		attsTwoDef := backup.QueryTableAtts{2, "j", false, true, false, "int", "", ""}
		attsThreeDef := backup.QueryTableAtts{3, "k", false, true, false, "int", "", ""}

		defaultsOne := backup.QueryTableDefault{1, "1"}
		defaultsTwo := backup.QueryTableDefault{2, "2"}
		defaultsThree := backup.QueryTableDefault{3, "3"}
		It("has no DEFAULT columns", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwo, attsThree}
			defaults := []backup.QueryTableDefault{}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (i)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwo, attsThree}
			defaults := []backup.QueryTableDefault{defaultsOne}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (j)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwoDef, attsThree}
			defaults := []backup.QueryTableDefault{defaultsTwo}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has one DEFAULT column (k)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwo, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and j)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwoDef, attsThree}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsTwo}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal(""))
		})
		It("has two DEFAULT columns (j and k)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwoDef, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsTwo, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal(""))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and k)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwo, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal(""))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
		It("has all DEFAULT columns", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwoDef, attsThreeDef}
			defaults := []backup.QueryTableDefault{defaultsOne, defaultsTwo, defaultsThree}
			info := backup.ConsolidateColumnInfo(atts, defaults)
			Expect(info[0].DefaultVal).To(Equal("1"))
			Expect(info[1].DefaultVal).To(Equal("2"))
			Expect(info[2].DefaultVal).To(Equal("3"))
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		buffer := gbytes.NewBuffer()
		baseSequence := utils.BasicRelation("public", "seq_name")
		commentSequence := utils.Relation{0, 0, "public", "seq_name", "This is a sequence comment.", ""}
		ownerSequence := utils.Relation{0, 0, "public", "seq_name", "", "testrole"}
		seqDefault := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqNegIncr := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, -1, -1, -9223372036854775807, 5, 42, false, true}}
		seqMaxPos := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, 1, 100, 1, 5, 42, false, true}}
		seqMinPos := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 10, 5, 42, false, true}}
		seqMaxNeg := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, -1, -10, -9223372036854775807, 5, 42, false, true}}
		seqMinNeg := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, -1, -1, -100, 5, 42, false, true}}
		seqCycle := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, true, true}}
		seqStart := backup.SequenceDefinition{baseSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, false}}
		seqComment := backup.SequenceDefinition{commentSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqOwner := backup.SequenceDefinition{ownerSequence, backup.QuerySequence{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}

		It("can print a sequence with all default options", func() {
			sequences := []backup.SequenceDefinition{seqDefault}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.SequenceDefinition{seqNegIncr}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.SequenceDefinition{seqMaxPos}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.SequenceDefinition{seqMinPos}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.SequenceDefinition{seqMaxNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.SequenceDefinition{seqMinNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.SequenceDefinition{seqCycle}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.SequenceDefinition{seqStart}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("can print a sequence with a comment", func() {
			sequences := []backup.SequenceDefinition{seqComment}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';`)
		})
		It("can print a sequence with an owner", func() {
			sequences := []backup.SequenceDefinition{seqOwner}
			backup.PrintCreateSequenceStatements(buffer, sequences)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


ALTER TABLE public.seq_name OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		buffer := gbytes.NewBuffer()

		It("can print schema with comments", func() {
			schemas := []utils.Schema{utils.Schema{0, "schema_with_comments", "This is a comment.", ""}}

			backup.PrintCreateSchemaStatements(buffer, schemas)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schema_with_comments;
COMMENT ON SCHEMA schema_with_comments IS 'This is a comment.';`)
		})
		It("can print schema with no comments", func() {
			schemas := []utils.Schema{utils.BasicSchema("schema_with_no_comments")}

			backup.PrintCreateSchemaStatements(buffer, schemas)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schema_with_no_comments;`)
		})
	})
	Describe("PrintSessionGUCs", func() {
		buffer := gbytes.NewBuffer()

		It("prints session GUCs", func() {
			gucs := backup.QuerySessionGUCs{"UTF8", "on", "false"}

			backup.PrintSessionGUCs(buffer, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})

	Describe("PrintDatabaseGUCs", func() {
		buffer := gbytes.NewBuffer()
		dbname := "testdb"
		defaultOidGUC := "default_with_oids=true"
		searchPathGUC := "search_path=pg_catalog, public"
		defaultStorageGUC := "gp_default_storage_options=appendonly=true,blocksize=32768"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids=true;`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids=true;
ALTER DATABASE testdb SET search_path=pg_catalog, public;
ALTER DATABASE testdb SET gp_default_storage_options=appendonly=true,blocksize=32768;`)
		})
	})
})

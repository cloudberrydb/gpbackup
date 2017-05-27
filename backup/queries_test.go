package backup_test

import (
	"database/sql/driver"
	"errors"
	"gpbackup/backup"
	"gpbackup/testutils"
	"gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/queries tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})

	Describe("SelectString", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal("one"))
		})
		It("returns an empty string if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(""))
		})
		It("panics if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testutils.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			backup.SelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("SelectStringSlice", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a slice containing a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal("one"))
		})
		It("returns an empty slice if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple strings if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("one"))
			Expect(results[1]).To(Equal("two"))
		})
	})
	Describe("GetTableAttributes", func() {
		header := []string{"attname", "attnotnull", "atthasdefault", "attisdropped", "atttypname", "attencoding", "attcomment"}
		rowOne := []driver.Value{"i", "f", "f", "f", "int", "", ""}
		rowTwo := []driver.Value{"j", "f", "f", "f", "character varying(20)", "", ""}
		rowEncoded := []driver.Value{"j", "f", "f", "f", "character varying(20)", "compresstype=zlib, blocksize=65536", ""}
		rowNotNull := []driver.Value{"j", "t", "f", "f", "character varying(20)", "", ""}

		It("returns a slice for a table with one column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAttributes(connection, 0)
			Expect(len(results)).To(Equal(1))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttHasDefault).To(Equal(false))
			Expect(results[0].AttIsDropped).To(Equal(false))
		})
		It("returns a slice for a table with two columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAttributes(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttTypName).To(Equal("int"))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttTypName).To(Equal("character varying(20)"))
		})
		It("returns a slice for a table with one NOT NULL column with ENCODING", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowEncoded...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAttributes(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttEncoding).To(Equal(""))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttEncoding).To(Equal("compresstype=zlib, blocksize=65536"))
		})
		It("returns a slice for a table with one NOT NULL column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowNotNull...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAttributes(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttEncoding).To(Equal(""))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttNotNull).To(Equal(true))
		})
		It("panics when table does not exist", func() {
			mock.ExpectQuery("SELECT (.*)").WillReturnError(errors.New("relation \"foo\" does not exist"))
			defer testutils.ShouldPanicWithMessage("relation \"foo\" does not exist")
			backup.GetTableAttributes(connection, 0)
		})
	})
	Describe("GetTableDefaults", func() {
		header := []string{"adnum", "defaultval"}
		rowOne := []driver.Value{"1", "42"}
		rowTwo := []driver.Value{"2", "bar"}

		It("returns a slice for a table with one column having a default value", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefaults(connection, 0)
			Expect(len(results)).To(Equal(1))
			Expect(results[0].AdNum).To(Equal(1))
			Expect(results[0].DefaultVal).To(Equal("42"))
		})
		It("returns a slice for a table with two columns having default values", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefaults(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AdNum).To(Equal(1))
			Expect(results[0].DefaultVal).To(Equal("42"))
			Expect(results[1].AdNum).To(Equal(2))
			Expect(results[1].DefaultVal).To(Equal("bar"))
		})
		It("returns a slice for a table with no columns having default values", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefaults(connection, 0)
			Expect(len(results)).To(Equal(0))
		})
	})
	Describe("GetConstraints", func() {
		header := []string{"conname", "condef"}
		rowOneUnique := []driver.Value{"relationname_i_uniq", "UNIQUE (i)"}
		rowTwoUnique := []driver.Value{"relationname_j_uniq", "UNIQUE (j)"}
		rowPrimarySingle := []driver.Value{"relationname_pkey", "PRIMARY KEY (i)"}
		rowPrimaryComposite := []driver.Value{"relationname_pkey", "PRIMARY KEY (i, j)"}
		rowOneForeign := []driver.Value{"relationname_i_fkey", "FOREIGN KEY (i) REFERENCES other_tablename(a)"}
		rowTwoForeign := []driver.Value{"relationname_j_fkey", "FOREIGN KEY (j) REFERENCES other_tablename(b)"}
		rowCheckSingle := []driver.Value{"check_i", "CHECK (i <> 42)"}
		rowCheckComposite := []driver.Value{"check_ij", "CHECK (i <> 42 AND j::text <> ''::text)"}

		Context("No constraints", func() {
			It("returns a slice for a table with no UNIQUE or PRIMARY KEY columns", func() {
				fakeResult := sqlmock.NewRows(header)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(0))
			})
		})
		Context("Columns with one constraint", func() {
			It("returns a slice for a table with one UNIQUE column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("relationname_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
			})
			It("returns a slice for a table with two UNIQUE columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowTwoUnique...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("relationname_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("relationname_j_uniq"))
				Expect(results[1].ConDef).To(Equal("UNIQUE (j)"))
			})
			It("returns a slice for a table with a PRIMARY KEY on one column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowPrimarySingle...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("relationname_pkey"))
				Expect(results[0].ConDef).To(Equal("PRIMARY KEY (i)"))
			})
			It("returns a slice for a table with a composite PRIMARY KEY on two columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowPrimaryComposite...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("relationname_pkey"))
				Expect(results[0].ConDef).To(Equal("PRIMARY KEY (i, j)"))
			})
			It("returns a slice for a table with one FOREIGN KEY column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneForeign...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("relationname_i_fkey"))
				Expect(results[0].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
			})
			It("returns a slice for a table with two FOREIGN KEY columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneForeign...).AddRow(rowTwoForeign...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("relationname_i_fkey"))
				Expect(results[0].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
				Expect(results[1].ConName).To(Equal("relationname_j_fkey"))
				Expect(results[1].ConDef).To(Equal("FOREIGN KEY (j) REFERENCES other_tablename(b)"))
			})
			It("returns a slice for a table with a CHECK on one column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowCheckSingle...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("check_i"))
				Expect(results[0].ConDef).To(Equal("CHECK (i <> 42)"))
			})
			It("returns a slice for a table with a composite CHECK on two columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowCheckComposite...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("check_ij"))
				Expect(results[0].ConDef).To(Equal("CHECK (i <> 42 AND j::text <> ''::text)"))
			})
		})
		Context("Tables with multiple constraints", func() {
			It("returns a slice for a table with one column having each simple constraint type", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowOneForeign...).AddRow(rowCheckSingle...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(3))
				Expect(results[0].ConName).To(Equal("relationname_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("relationname_i_fkey"))
				Expect(results[1].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
				Expect(results[2].ConName).To(Equal("check_i"))
				Expect(results[2].ConDef).To(Equal("CHECK (i <> 42)"))
			})
			It("returns a slice for a table with one column having each complex constraint type", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneForeign...).AddRow(rowPrimaryComposite...).AddRow(rowCheckComposite...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetConstraints(connection, 0)
				Expect(len(results)).To(Equal(3))
				Expect(results[0].ConName).To(Equal("relationname_i_fkey"))
				Expect(results[0].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
				Expect(results[1].ConName).To(Equal("relationname_pkey"))
				Expect(results[1].ConDef).To(Equal("PRIMARY KEY (i, j)"))
				Expect(results[2].ConName).To(Equal("check_ij"))
				Expect(results[2].ConDef).To(Equal("CHECK (i <> 42 AND j::text <> ''::text)"))
			})
		})
	})
	Describe("GetAllUserTables", func() {
		header := []string{"schemaoid", "schemaname", "relationoid", "relationname", "comment", "owner"}
		tableOne := []driver.Value{0, "public", 1, "table_one", "", ""}
		tableTwo := []driver.Value{0, "public", 2, "table_two", "This is a comment.", ""}
		tableThree := []driver.Value{1, "testschema", 3, "table_three", "", "testrole"}

		It("returns a slice of tables with schemas, comments, and owners", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(tableOne...).AddRow(tableTwo...).AddRow(tableThree...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetAllUserTables(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0].SchemaOid).To(Equal(uint32(0)))
			Expect(results[0].SchemaName).To(Equal("public"))
			Expect(results[0].RelationOid).To(Equal(uint32(1)))
			Expect(results[0].RelationName).To(Equal("table_one"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal(""))
			Expect(results[1].SchemaOid).To(Equal(uint32(0)))
			Expect(results[1].SchemaName).To(Equal("public"))
			Expect(results[1].RelationOid).To(Equal(uint32(2)))
			Expect(results[1].RelationName).To(Equal("table_two"))
			Expect(results[1].Comment).To(Equal("This is a comment."))
			Expect(results[1].Owner).To(Equal(""))
			Expect(results[2].SchemaOid).To(Equal(uint32(1)))
			Expect(results[2].SchemaName).To(Equal("testschema"))
			Expect(results[2].RelationOid).To(Equal(uint32(3)))
			Expect(results[2].RelationName).To(Equal("table_three"))
			Expect(results[2].Comment).To(Equal(""))
			Expect(results[2].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetDistributionPolicy", func() {
		header := []string{"string"}
		rowDistOne := []driver.Value{"i"}
		rowDistTwo := []driver.Value{"j"}

		It("returns a slice for a table DISTRIBUTED RANDOMLY", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetDistributionPolicy(connection, 0)
			Expect(results).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns a slice for a table DISTRIBUTED BY one column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowDistOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetDistributionPolicy(connection, 0)
			Expect(results).To(Equal("DISTRIBUTED BY (i)"))
		})
		It("returns a slice for a table DISTRIBUTED BY two columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowDistOne...).AddRow(rowDistTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetDistributionPolicy(connection, 0)
			Expect(results).To(Equal("DISTRIBUTED BY (i, j)"))
		})
	})
	Describe("GetAllSequences", func() {
		header := []string{"schemaoid", "schemaname", "relationoid", "relationname", "comment"}
		withoutCommentOne := []driver.Value{0, "public", 1, "seq_one", ""}
		withoutCommentTwo := []driver.Value{0, "public", 2, "seq_two", ""}
		withCommentOne := []driver.Value{0, "public", 1, "seq_one", "This is a sequence comment."}
		withCommentTwo := []driver.Value{0, "public", 2, "seq_two", "This is another sequence comment."}

		It("returns a slice of sequences", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(withoutCommentOne...).AddRow(withoutCommentTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetAllSequences(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].SchemaOid).To(Equal(uint32(0)))
			Expect(results[0].SchemaName).To(Equal("public"))
			Expect(results[0].RelationOid).To(Equal(uint32(1)))
			Expect(results[0].RelationName).To(Equal("seq_one"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[1].SchemaOid).To(Equal(uint32(0)))
			Expect(results[1].SchemaName).To(Equal("public"))
			Expect(results[1].RelationOid).To(Equal(uint32(2)))
			Expect(results[1].RelationName).To(Equal("seq_two"))
			Expect(results[1].Comment).To(Equal(""))
		})
		It("returns a slice of sequences with comments", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(withCommentOne...).AddRow(withCommentTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetAllSequences(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].RelationName).To(Equal("seq_one"))
			Expect(results[0].Comment).To(Equal("This is a sequence comment."))
			Expect(results[1].RelationName).To(Equal("seq_two"))
			Expect(results[1].Comment).To(Equal("This is another sequence comment."))
		})
	})
	Describe("GetAllSequenceDefinitions", func() {
		headerSeq := []string{"schemaoid", "schemaname", "relationoid", "relationname", "comment", "owner"}
		seqOne := []driver.Value{0, "public", 1, "seq_one", "This is a comment.", ""}
		seqTwo := []driver.Value{0, "public", 2, "seq_two", "", "testrole"}
		headerSeqDef := []string{"sequence_name", "last_value", "increment_by", "max_value", "min_value", "cache_value", "log_cnt", "is_cycled", "is_called"}
		seqDefOne := []driver.Value{"public.seq_one", 3, 1, 1000, 1, 2, 41, "f", "f"}
		seqDefTwo := []driver.Value{"public.seq_two", 7, 1, 9223372036854775807, 1, 5, 42, "f", "f"}

		It("returns a slice of definitions for all sequences", func() {
			fakeSequences := sqlmock.NewRows(headerSeq).AddRow(seqOne...).AddRow(seqTwo...)
			fakeResultOne := sqlmock.NewRows(headerSeqDef).AddRow(seqDefOne...)
			fakeResultTwo := sqlmock.NewRows(headerSeqDef).AddRow(seqDefTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeSequences)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResultOne)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResultTwo)
			results := backup.GetAllSequenceDefinitions(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].Name).To(Equal("public.seq_one"))
			Expect(results[0].LastVal).To(Equal(int64(3)))
			Expect(results[0].Increment).To(Equal(int64(1)))
			Expect(results[0].Comment).To(Equal("This is a comment."))
			Expect(results[0].Owner).To(Equal(""))
			Expect(results[1].Name).To(Equal("public.seq_two"))
			Expect(results[1].LastVal).To(Equal(int64(7)))
			Expect(results[1].Increment).To(Equal(int64(1)))
			Expect(results[1].Comment).To(Equal(""))
			Expect(results[1].Owner).To(Equal("testrole"))
		})
	})
	Describe("GetAllUserSchemas", func() {
		headerSchema := []string{"schemaoid", "schemaname", "comment", "owner"}
		schemaOne := []driver.Value{1, "schema_one", "", "testrole"}
		schemaTwo := []driver.Value{2, "schema_two", "some_comment", ""}

		It("returns a slice of definitions for all schemas", func() {
			fakeSchema := sqlmock.NewRows(headerSchema).AddRow(schemaOne...).AddRow(schemaTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeSchema)
			results := backup.GetAllUserSchemas(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].SchemaOid).To(Equal(uint32(1)))
			Expect(results[0].SchemaName).To(Equal("schema_one"))
			Expect(results[0].Comment).To(Equal(""))
			Expect(results[0].Owner).To(Equal("testrole"))
			Expect(results[1].SchemaOid).To(Equal(uint32(2)))
			Expect(results[1].SchemaName).To(Equal("schema_two"))
			Expect(results[1].Comment).To(Equal("some_comment"))
			Expect(results[1].Owner).To(Equal(""))
		})
	})
	Describe("GetSessionGUCs", func() {
		headerEncoding := []string{"client_encoding"}
		rowEncoding := []driver.Value{"UTF8"}
		headerStrings := []string{"standard_conforming_strings"}
		rowStrings := []driver.Value{"on"}
		headerOids := []string{"default_with_oids"}
		rowOids := []driver.Value{"false"}

		It("returns a slice of values for session level GUCs", func() {
			fakeEncoding := sqlmock.NewRows(headerEncoding).AddRow(rowEncoding...)
			fakeStrings := sqlmock.NewRows(headerStrings).AddRow(rowStrings...)
			fakeOids := sqlmock.NewRows(headerOids).AddRow(rowOids...)

			mock.ExpectQuery("SHOW cl.*").WillReturnRows(fakeEncoding)
			mock.ExpectQuery("SHOW sta.*").WillReturnRows(fakeStrings)
			mock.ExpectQuery("SHOW def.*").WillReturnRows(fakeOids)
			results := backup.GetSessionGUCs(connection)
			Expect(results.ClientEncoding).To(Equal("UTF8"))
			Expect(results.StdConformingStrings).To(Equal("on"))
			Expect(results.DefaultWithOids).To(Equal("false"))
		})
	})
	Describe("GetSequence", func() {
		header := []string{"sequence_name", "last_value", "increment_by", "max_value", "min_value", "cache_value", "log_cnt", "is_cycled", "is_called"}
		sequenceOne := []driver.Value{"seq_name", "42", 1, 1000, 1, 41, 2, false, false}

		It("returns a slice for a sequence", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(sequenceOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.GetSequence(connection, "SELECT * FROM foo")
			Expect(result.Name).To(Equal("seq_name"))
			Expect(result.LastVal).To(Equal(int64(42)))
			Expect(result.Increment).To(Equal(int64(1)))
			Expect(result.MaxVal).To(Equal(int64(1000)))
			Expect(result.MinVal).To(Equal(int64(1)))
			Expect(result.CacheVal).To(Equal(int64(41)))
			Expect(result.LogCnt).To(Equal(int64(2)))
			Expect(result.IsCycled).To(BeFalse())
			Expect(result.IsCalled).To(BeFalse())
		})
	})
	Describe("GetProceduralLanguages", func() {
		header := []string{"lanname", "owner", "lanispl", "lanpltrusted", "handler", "inline", "validator", "lanacl", "comment"}
		procLangOne := []driver.Value{"plpythonu", "public", "t", "f", "handler_func", "inline_func", "validator_func", "", "comment"}

		It("returns a slice of tables with schemas and comments", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(procLangOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.GetProceduralLanguages(connection)
			Expect(result[0].Name).To(Equal("plpythonu"))
			Expect(result[0].Owner).To(Equal("public"))
			Expect(result[0].IsPl).To(BeTrue())
			Expect(result[0].PlTrusted).To(BeFalse())
			Expect(result[0].Handler).To(Equal("handler_func"))
			Expect(result[0].Inline).To(Equal("inline_func"))
			Expect(result[0].Validator).To(Equal("validator_func"))
			Expect(result[0].Access).To(Equal(""))
			Expect(result[0].Comment).To(Equal("comment"))
		})
	})
})

package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
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
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := utils.BasicRelation("public", "seq_name")
		commentSequence := utils.Relation{0, 0, "public", "seq_name", "This is a sequence comment.", ""}
		ownerSequence := utils.Relation{0, 0, "public", "seq_name", "", "testrole"}
		seqDefault := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqNegIncr := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -9223372036854775807, 5, 42, false, true}}
		seqMaxPos := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 100, 1, 5, 42, false, true}}
		seqMinPos := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 10, 5, 42, false, true}}
		seqMaxNeg := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -10, -9223372036854775807, 5, 42, false, true}}
		seqMinNeg := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -100, 5, 42, false, true}}
		seqCycle := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, true, true}}
		seqStart := backup.Sequence{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, false}}
		seqComment := backup.Sequence{commentSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqOwner := backup.Sequence{ownerSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		emptyOwnerMap := make(map[string]string, 0)

		It("can print a sequence with all default options", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
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
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("can print a sequence with a comment", func() {
			sequences := []backup.Sequence{seqComment}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';`)
		})
		It("can print a sequence with an owner", func() {
			sequences := []backup.Sequence{seqOwner}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


ALTER TABLE public.seq_name OWNER TO testrole;`)
		})
		It("can print a sequence with an owning column", func() {
			sequences := []backup.Sequence{seqOwner}
			ownerMap := map[string]string{"public.seq_name": "tablename.col_one"}
			backup.PrintCreateSequenceStatements(buffer, sequences, ownerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


ALTER TABLE public.seq_name OWNER TO testrole;


ALTER SEQUENCE public.seq_name OWNED BY tablename.col_one`)
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print schema with comments", func() {
			schemas := []utils.Schema{{0, "schema_with_comments", "This is a comment.", ""}}

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
	Describe("PrintCreateLanguageStatements", func() {
		plUntrustedHandlerOnly := backup.QueryProceduralLanguage{"plpythonu", "testrole", true, false, 4, 0, 0, "", ""}
		plAllFields := backup.QueryProceduralLanguage{"plpgsql", "testrole", true, true, 1, 2, 3, "", ""}
		plComment := backup.QueryProceduralLanguage{"plpythonu", "testrole", true, false, 4, 0, 0, "", "language comment"}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.plpgsql_call_handler", Arguments: "", IsInternal: true},
			2: {QualifiedName: "pg_catalog.plpgsql_inline_handler", Arguments: "internal", IsInternal: true},
			3: {QualifiedName: "pg_catalog.plpgsql_validator", Arguments: "oid", IsInternal: true},
			4: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: "", IsInternal: true},
		}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;`)
		})
		It("prints trusted language with handler, inline, validator, and comments", func() {
			langs := []backup.QueryProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;
ALTER LANGUAGE plpgsql OWNER TO testrole;`)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;


CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;
ALTER LANGUAGE plpgsql OWNER TO testrole;`)
		})
		It("prints language with comment", func() {
			langs := []backup.QueryProceduralLanguage{plComment}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;

COMMENT ON LANGUAGE plpythonu IS 'language comment'`)
		})
	})
	Describe("PrintCreateViewStatements", func() {
		It("prints create view statement", func() {
			viewOne := backup.QueryViewDefinition{"public", "WowZa", "SELECT rolname FROM pg_role;", ""}
			viewTwo := backup.QueryViewDefinition{"shamwow", "shazam", "SELECT count(*) FROM pg_tables;", "this is a view comment"}
			backup.PrintCreateViewStatements(buffer, []backup.QueryViewDefinition{viewOne, viewTwo})
			testutils.ExpectRegexp(buffer, `CREATE VIEW public."WowZa" AS SELECT rolname FROM pg_role;


CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;

COMMENT ON VIEW shamwow.shazam IS 'this is a view comment';
`)
		})

	})
	Describe("PrintExternalProtocolStatements", func() {
		protocolUntrustedReadWrite := backup.QueryExtProtocol{"s3", "testrole", false, 1, 2, 0, ""}
		protocolUntrustedReadValidator := backup.QueryExtProtocol{"s3", "testrole", false, 1, 0, 3, ""}
		protocolUntrustedWriteOnly := backup.QueryExtProtocol{"s3", "testrole", false, 0, 2, 0, ""}
		protocolTrustedReadWriteValidator := backup.QueryExtProtocol{"s3", "testrole", true, 1, 2, 3, ""}
		protocolUntrustedReadOnly := backup.QueryExtProtocol{"s4", "testrole", false, 4, 0, 0, ""}
		protocolInternal := backup.QueryExtProtocol{"gphdfs", "testrole", false, 5, 6, 7, ""}
		protocolInternalReadWrite := backup.QueryExtProtocol{"gphdfs", "testrole", false, 5, 6, 0, ""}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.read_fn_s3", Arguments: ""},
			2: {QualifiedName: "public.write_fn_s3", Arguments: ""},
			3: {QualifiedName: "public.validator", Arguments: ""},
			4: {QualifiedName: "public.read_fn_s4", Arguments: ""},
			5: {QualifiedName: "pg_catalog.read_internal_fn", Arguments: "", IsInternal: true},
			6: {QualifiedName: "pg_catalog.write_internal_fn", Arguments: "", IsInternal: true},
			7: {QualifiedName: "pg_catalog.validate_internal_fn", Arguments: "", IsInternal: true},
		}

		It("prints untrusted protocol with read and write function", func() {
			protos := []backup.QueryExtProtocol{protocolUntrustedReadWrite}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3);

ALTER PROTOCOL s3 OWNER TO testrole;`)
		})
		It("prints untrusted protocol with read and validator", func() {
			protos := []backup.QueryExtProtocol{protocolUntrustedReadValidator}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, validatorfunc = public.validator);

ALTER PROTOCOL s3 OWNER TO testrole;`)
		})
		It("prints untrusted protocol with write function only", func() {
			protos := []backup.QueryExtProtocol{protocolUntrustedWriteOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s3 (writefunc = public.write_fn_s3);

ALTER PROTOCOL s3 OWNER TO testrole;`)
		})
		It("prints trusted protocol with read, write, and validator", func() {
			protos := []backup.QueryExtProtocol{protocolTrustedReadWriteValidator}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE TRUSTED PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3, validatorfunc = public.validator);

ALTER PROTOCOL s3 OWNER TO testrole;`)
		})
		It("prints multiple protocols", func() {
			protos := []backup.QueryExtProtocol{protocolUntrustedWriteOnly, protocolUntrustedReadOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s3 (writefunc = public.write_fn_s3);

ALTER PROTOCOL s3 OWNER TO testrole;


CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);

ALTER PROTOCOL s4 OWNER TO testrole;`)
		})
		It("skips printing protocols where all functions are internal", func() {
			protos := []backup.QueryExtProtocol{protocolInternal, protocolUntrustedReadOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.NotExpectRegexp(buffer, `CREATE PROTOCOL gphdfs`)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);

ALTER PROTOCOL s4 OWNER TO testrole;`)
		})
		It("skips printing protocols without validator where all functions are internal", func() {
			protos := []backup.QueryExtProtocol{protocolInternalReadWrite, protocolUntrustedReadOnly}

			backup.PrintCreateExternalProtocolStatements(buffer, protos, funcInfoMap)
			testutils.NotExpectRegexp(buffer, `CREATE PROTOCOL gphdfs`)
			testutils.ExpectRegexp(buffer, `CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);

ALTER PROTOCOL s4 OWNER TO testrole;`)
		})
	})
})

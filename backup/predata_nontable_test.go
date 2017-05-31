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
SET client_min_messages = error;
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

	Describe("PrintCreateLanguageStatements", func() {
		buffer := gbytes.NewBuffer()
		plUntrustedHandlerOnly := backup.QueryProceduralLanguage{"plpythonu", "public", true, false, "plpython_call_handler()", "", "", "", ""}
		plAllFields := backup.QueryProceduralLanguage{"plpgsql", "public", true, true, "plpgsql_call_handler()", "plpgsql_inline_handler(internal)", "plpgsql_validator(oid)", "", ""}
		plComment := backup.QueryProceduralLanguage{"plpythonu", "public", true, false, "plpython_call_handler()", "", "", "", "language comment"}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(buffer, langs)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION plpython_call_handler() OWNER TO public;
ALTER LANGUAGE plpythonu OWNER TO public;`)
		})
		It("prints trusted language with handler, inline, validator, and comments", func() {
			langs := []backup.QueryProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs)
			testutils.ExpectRegexp(buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION plpgsql_call_handler() OWNER TO public;
ALTER FUNCTION plpgsql_inline_handler(internal) OWNER TO public;
ALTER FUNCTION plpgsql_validator(oid) OWNER TO public;
ALTER LANGUAGE plpgsql OWNER TO public;`)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION plpython_call_handler() OWNER TO public;
ALTER LANGUAGE plpythonu OWNER TO public;

CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION plpgsql_call_handler() OWNER TO public;
ALTER FUNCTION plpgsql_inline_handler(internal) OWNER TO public;
ALTER FUNCTION plpgsql_validator(oid) OWNER TO public;
ALTER LANGUAGE plpgsql OWNER TO public;`)
		})
		It("prints language with comment", func() {
			langs := []backup.QueryProceduralLanguage{plComment}

			backup.PrintCreateLanguageStatements(buffer, langs)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION plpython_call_handler() OWNER TO public;
ALTER LANGUAGE plpythonu OWNER TO public;

COMMENT ON LANGUAGE plpythonu IS 'language comment'`)
		})
	})
})

package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_general tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintConstraintStatements", func() {
		uniqueOne := backup.QueryConstraint{1, "tablename_i_key", "u", "UNIQUE (i)", "public.tablename"}
		uniqueTwo := backup.QueryConstraint{0, "tablename_j_key", "u", "UNIQUE (j)", "public.tablename"}
		primarySingle := backup.QueryConstraint{0, "tablename_pkey", "p", "PRIMARY KEY (i)", "public.tablename"}
		primaryComposite := backup.QueryConstraint{0, "tablename_pkey", "p", "PRIMARY KEY (i, j)", "public.tablename"}
		foreignOne := backup.QueryConstraint{0, "tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)", "public.tablename"}
		foreignTwo := backup.QueryConstraint{0, "tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)", "public.tablename"}
		emptyMetadataMap := backup.MetadataMap{}

		Context("No constraints", func() {
			It("doesn't print anything", func() {
				constraints := []backup.QueryConstraint{}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.NotExpectRegexp(buffer, `CONSTRAINT`)
			})
		})
		Context("Constraints involving different columns", func() {
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint with a comment", func() {
				constraints := []backup.QueryConstraint{uniqueOne}
				constraintMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
				backup.PrintConstraintStatements(buffer, constraints, constraintMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';
`)
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);
`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				constraints := []backup.QueryConstraint{uniqueOne, uniqueTwo}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);
`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				constraints := []backup.QueryConstraint{primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);
`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.QueryConstraint{primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);
`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				constraints := []backup.QueryConstraint{foreignOne, foreignTwo}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignTwo, uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignTwo, primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignTwo, primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne, uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne, primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne, primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print a basic schema", func() {
			schemas := []utils.Schema{{0, "schemaname"}}
			emptyMetadataMap := backup.MetadataMap{}

			backup.PrintCreateSchemaStatements(buffer, schemas, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schemaname;`)
		})
		It("can print a schema with privileges, an owner, and a comment", func() {
			schemas := []utils.Schema{{1, "schemaname"}}
			schemaMetadataMap := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(buffer, schemas, schemaMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schemaname;

COMMENT ON SCHEMA schemaname IS 'This is a schema comment.';


ALTER SCHEMA schemaname OWNER TO testrole;


REVOKE ALL ON SCHEMA schemaname FROM PUBLIC;
REVOKE ALL ON SCHEMA schemaname FROM testrole;
GRANT ALL ON SCHEMA schemaname TO testrole;`)
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		plUntrustedHandlerOnly := backup.QueryProceduralLanguage{1, "plpythonu", "testrole", true, false, 4, 0, 0}
		plAllFields := backup.QueryProceduralLanguage{1, "plpgsql", "testrole", true, true, 1, 2, 3}
		plComment := backup.QueryProceduralLanguage{1, "plpythonu", "testrole", true, false, 4, 0, 0}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.plpgsql_call_handler", Arguments: "", IsInternal: true},
			2: {QualifiedName: "pg_catalog.plpgsql_inline_handler", Arguments: "internal", IsInternal: true},
			3: {QualifiedName: "pg_catalog.plpgsql_validator", Arguments: "oid", IsInternal: true},
			4: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: "", IsInternal: true},
		}
		emptyMetadataMap := backup.MetadataMap{}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;`)
		})
		It("prints trusted language with handler, inline, and validator", func() {
			langs := []backup.QueryProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;`)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;


CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;`)
		})
		It("prints a language with privileges, an owner, and a comment", func() {
			langs := []backup.QueryProceduralLanguage{plComment}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap, langMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;

COMMENT ON LANGUAGE plpythonu IS 'This is a language comment.';


ALTER LANGUAGE plpythonu OWNER TO testrole;


REVOKE ALL ON LANGUAGE plpythonu FROM PUBLIC;
REVOKE ALL ON LANGUAGE plpythonu FROM testrole;
GRANT ALL ON LANGUAGE plpythonu TO testrole;`)
		})
	})
})

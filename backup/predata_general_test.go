package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_general tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintConstraintStatements", func() {
		uniqueOne := backup.QueryConstraint{1, "tablename_i_key", "u", "UNIQUE (i)", "public.tablename", false}
		uniqueTwo := backup.QueryConstraint{0, "tablename_j_key", "u", "UNIQUE (j)", "public.tablename", false}
		primarySingle := backup.QueryConstraint{0, "tablename_pkey", "p", "PRIMARY KEY (i)", "public.tablename", false}
		primaryComposite := backup.QueryConstraint{0, "tablename_pkey", "p", "PRIMARY KEY (i, j)", "public.tablename", false}
		foreignOne := backup.QueryConstraint{0, "tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)", "public.tablename", false}
		foreignTwo := backup.QueryConstraint{0, "tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)", "public.tablename", false}
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
			It("prints ADD CONSTRAINT statement for domain check constraint", func() {
				domainCheckConstraint := backup.QueryConstraint{0, "check1", "c", "CHECK (VALUE <> 42::numeric)", "public.domain1", true}
				constraints := []backup.QueryConstraint{domainCheckConstraint}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER DOMAIN public.domain1 ADD CONSTRAINT check1 CHECK (VALUE <> 42::numeric);
`)
			})
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print a basic schema", func() {
			schemas := []backup.Schema{{0, "schemaname"}}
			emptyMetadataMap := backup.MetadataMap{}

			backup.PrintCreateSchemaStatements(buffer, schemas, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schemaname;`)
		})
		It("can print a schema with privileges, an owner, and a comment", func() {
			schemas := []backup.Schema{{1, "schemaname"}}
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
	Describe("ExtractLanguageFunctions", func() {
		customLang := backup.QueryProceduralLanguage{1, "custom_language", "testrole", true, true, 3, 4, 5}
		procLangs := []backup.QueryProceduralLanguage{customLang}
		langFunc := backup.QueryFunctionDefinition{Oid: 3, FunctionName: "custom_handler"}
		nonLangFunc := backup.QueryFunctionDefinition{Oid: 2, FunctionName: "random_function"}
		It("handles a case where all functions are language-associated functions", func() {
			funcDefs := []backup.QueryFunctionDefinition{langFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(1))
			Expect(len(otherFuncs)).To(Equal(0))
			Expect(langFuncs[0].FunctionName).To(Equal("custom_handler"))
		})
		It("handles a case where no functions are language-associated functions", func() {
			funcDefs := []backup.QueryFunctionDefinition{nonLangFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(0))
			Expect(len(otherFuncs)).To(Equal(1))
			Expect(otherFuncs[0].FunctionName).To(Equal("random_function"))
		})
		It("handles a case where some functions are language-associated functions", func() {
			funcDefs := []backup.QueryFunctionDefinition{langFunc, nonLangFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(1))
			Expect(len(otherFuncs)).To(Equal(1))
			Expect(langFuncs[0].FunctionName).To(Equal("custom_handler"))
			Expect(otherFuncs[0].FunctionName).To(Equal("random_function"))
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
	Describe("PrintCreateOperatorStatements", func() {
		It("prints a basic operator", func() {
			operator := backup.QueryOperator{0, "public", "##", "public.path_inter", "public.path", "public.path", "0", "0", "-", "-", false, false}

			backup.PrintCreateOperatorStatements(buffer, []backup.QueryOperator{operator}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path
);`)
		})
		It("prints a full-featured operator", func() {
			operator := backup.QueryOperator{1, "testschema", "##", "public.path_inter", "public.path", "public.path", "testschema.##", "testschema.###", "eqsel(internal,oid,internal,integer)", "eqjoinsel(internal,oid,internal,smallint)", true, true}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(buffer, []backup.QueryOperator{operator}, metadataMap)

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR testschema.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path,
	COMMUTATOR = OPERATOR(testschema.##),
	NEGATOR = OPERATOR(testschema.###),
	RESTRICT = eqsel(internal,oid,internal,integer),
	JOIN = eqjoinsel(internal,oid,internal,smallint),
	HASHES,
	MERGES
);

COMMENT ON OPERATOR testschema.## (public.path, public.path) IS 'This is an operator comment.';


ALTER OPERATOR testschema.## (public.path, public.path) OWNER TO testrole;`)
		})
		It("prints an operator with only a left argument", func() {
			operator := backup.QueryOperator{1, "public", "##", "public.path_inter", "public.path", "-", "0", "0", "-", "-", false, false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(buffer, []backup.QueryOperator{operator}, metadataMap)

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path
);

COMMENT ON OPERATOR public.## (public.path, NONE) IS 'This is an operator comment.';


ALTER OPERATOR public.## (public.path, NONE) OWNER TO testrole;`)
		})
		It("prints an operator with only a right argument", func() {
			operator := backup.QueryOperator{1, "public", "##", "public.path_inter", "-", "public.\"PATH\"", "0", "0", "-", "-", false, false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(buffer, []backup.QueryOperator{operator}, metadataMap)

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	RIGHTARG = public."PATH"
);

COMMENT ON OPERATOR public.## (NONE, public."PATH") IS 'This is an operator comment.';


ALTER OPERATOR public.## (NONE, public."PATH") OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("prints a basic operator family", func() {
			operatorFamily := backup.QueryOperatorFamily{0, "public", "testfam", "hash"}

			backup.PrintCreateOperatorFamilyStatements(buffer, []backup.QueryOperatorFamily{operatorFamily}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;`)
		})
		It("prints an operator family with an owner and comment", func() {
			operatorFamily := backup.QueryOperatorFamily{1, "public", "testfam", "hash"}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)

			backup.PrintCreateOperatorFamilyStatements(buffer, []backup.QueryOperatorFamily{operatorFamily}, metadataMap)

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;

COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.';


ALTER OPERATOR FAMILY public.testfam USING hash OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("prints a basic operator class", func() {
			operatorClass := backup.QueryOperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	STORAGE uuid;`)
		})
		It("prints an operator class with default and family", func() {
			operatorClass := backup.QueryOperatorClass{0, "public", "testclass", "public", "testfam", "hash", "uuid", true, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS public.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY public.testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with class and family in different schemas", func() {
			operatorClass := backup.QueryOperatorClass{0, "schema1", "testclass", "Schema2", "testfam", "hash", "uuid", true, "-", nil, nil}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS schema1.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY "Schema2".testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with an operator", func() {
			operatorClass := backup.QueryOperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", false}}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid);`)
		})
		It("prints an operator class with two operators and recheck", func() {
			operatorClass := backup.QueryOperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{0, 1, "=(uuid,uuid)", true}, {0, 2, ">(uuid,uuid)", false}}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid) RECHECK,
	OPERATOR 2 >(uuid,uuid);`)
		})
		It("prints an operator class with a function", func() {
			operatorClass := backup.QueryOperatorClass{0, "public", "testclass", "public", "testclass", "hash", "uuid", false, "-", nil, nil}
			operatorClass.Functions = []backup.OperatorClassFunction{{0, 1, "abs(integer)"}}

			backup.PrintCreateOperatorClassStatements(buffer, []backup.QueryOperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectRegexp(buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 abs(integer);`)
		})
	})
})

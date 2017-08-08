package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_shared tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})

	Describe("PrintConstraintStatements", func() {
		var (
			uniqueOne        backup.Constraint
			uniqueTwo        backup.Constraint
			primarySingle    backup.Constraint
			primaryComposite backup.Constraint
			foreignOne       backup.Constraint
			foreignTwo       backup.Constraint
			emptyMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			uniqueOne = backup.Constraint{1, "tablename_i_key", "u", "UNIQUE (i)", "public.tablename", false, false}
			uniqueTwo = backup.Constraint{0, "tablename_j_key", "u", "UNIQUE (j)", "public.tablename", false, false}
			primarySingle = backup.Constraint{0, "tablename_pkey", "p", "PRIMARY KEY (i)", "public.tablename", false, false}
			primaryComposite = backup.Constraint{0, "tablename_pkey", "p", "PRIMARY KEY (i, j)", "public.tablename", false, false}
			foreignOne = backup.Constraint{0, "tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)", "public.tablename", false, false}
			foreignTwo = backup.Constraint{0, "tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)", "public.tablename", false, false}
			emptyMetadataMap = backup.MetadataMap{}
		})

		Context("No constraints", func() {
			It("doesn't print anything", func() {
				constraints := []backup.Constraint{}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.NotExpectRegexp(buffer, `CONSTRAINT`)
			})
		})
		Context("Constraints involving different columns", func() {
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint with a comment", func() {
				constraints := []backup.Constraint{uniqueOne}
				constraintMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
				backup.PrintConstraintStatements(buffer, constraints, constraintMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';
`)
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				constraints := []backup.Constraint{uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);
`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				constraints := []backup.Constraint{uniqueOne, uniqueTwo}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);
`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				constraints := []backup.Constraint{primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);
`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.Constraint{primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);
`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				constraints := []backup.Constraint{foreignOne, foreignTwo}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);
`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primarySingle}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primaryComposite}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);


ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);
`)
			})
			It("prints ADD CONSTRAINT statement for domain check constraint", func() {
				domainCheckConstraint := backup.Constraint{0, "check1", "c", "CHECK (VALUE <> 42::numeric)", "public.domain1", true, false}
				constraints := []backup.Constraint{domainCheckConstraint}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER DOMAIN public.domain1 ADD CONSTRAINT check1 CHECK (VALUE <> 42::numeric);
`)
			})
			It("prints an ADD CONSTRAINT statement for a parent partition table", func() {
				uniqueOne.IsPartitionParent = true
				constraints := []backup.Constraint{uniqueOne}
				backup.PrintConstraintStatements(buffer, constraints, emptyMetadataMap)
				testutils.ExpectRegexp(buffer, `

ALTER TABLE public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);
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
	Describe("Schema.ToString", func() {
		It("remains unquoted if it contains no special characters", func() {
			testSchema := backup.Schema{0, `schemaname`}
			expected := `schemaname`
			Expect(testSchema.ToString()).To(Equal(expected))
		})
		It("is quoted if it contains special characters", func() {
			testSchema := backup.Schema{0, `schema,name`}
			expected := `"schema,name"`
			Expect(testSchema.ToString()).To(Equal(expected))
		})
	})
	Describe("SchemaFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname`
			newSchema := backup.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schemaname`))
		})
		It("can parse a quoted string", func() {
			testString := `"schema,name"`
			newSchema := backup.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schema,name`))
		})
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
	})
	Describe("GetUniqueSchemas", func() {
		alphabeticalAFoo := backup.Relation{1, 0, "otherschema", "foo", nil, nil}
		alphabeticalABar := backup.Relation{1, 0, "otherschema", "bar", nil, nil}
		schemaOther := backup.Schema{2, "otherschema"}
		alphabeticalBFoo := backup.Relation{2, 0, "public", "foo", nil, nil}
		alphabeticalBBar := backup.Relation{2, 0, "public", "bar", nil, nil}
		schemaPublic := backup.Schema{1, "public"}
		schemas := []backup.Schema{schemaOther, schemaPublic}

		It("has multiple tables in a single schema", func() {
			tables := []backup.Relation{alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{schemaPublic}))
		})
		It("has multiple schemas, each with multiple tables", func() {
			tables := []backup.Relation{alphabeticalBFoo, alphabeticalBBar, alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{schemaOther, schemaPublic}))
		})
		It("has no tables", func() {
			tables := []backup.Relation{}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{}))
		})
	})
	Describe("PrintObjectMetadata", func() {
		hasAllPrivileges := testutils.DefaultACLForType("anothertestrole", "TABLE")
		hasMostPrivileges := testutils.DefaultACLForType("testrole", "TABLE")
		hasMostPrivileges.Trigger = false
		hasSinglePrivilege := backup.ACL{Grantee: "", Trigger: true}
		hasAllPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("anothertestrole", "TABLE")
		hasMostPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("testrole", "TABLE")
		hasMostPrivilegesWithGrant.TriggerWithGrant = false
		hasSinglePrivilegeWithGrant := backup.ACL{Grantee: "", TriggerWithGrant: true}
		privileges := []backup.ACL{hasAllPrivileges, hasMostPrivileges, hasSinglePrivilege}
		privilegesWithGrant := []backup.ACL{hasAllPrivilegesWithGrant, hasMostPrivilegesWithGrant, hasSinglePrivilegeWithGrant}
		It("prints a block with a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment."}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a block of REVOKE and GRANT statements", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints a block of REVOKE and GRANT statements WITH GRANT OPTION", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privilegesWithGrant}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole WITH GRANT OPTION;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC WITH GRANT OPTION;`)
		})
		It("prints a block of REVOKE and GRANT statements, some with WITH GRANT OPTION, some without", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{hasAllPrivileges, hasMostPrivilegesWithGrant}}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment.", Owner: "testrole"}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both a block of REVOKE and GRANT statements and an ALTER TABLE ... OWNER TO statement", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Owner: "testrole"}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints both a block of REVOKE and GRANT statements and a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Comment: "This is a table comment."}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints REVOKE and GRANT statements, an ALTER TABLE ... OWNER TO statement, and comments", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintObjectMetadata(buffer, tableMetadata, "public.tablename", "TABLE")
			testutils.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
	})
})

package backup_test

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_shared tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("QuoteIdent", func() {
		It("does not quote a string that contains no special characters", func() {
			name := `_tablename1`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`_tablename1`))
		})
		It("replaces double quotes with pairs of double quotes", func() {
			name := `table"name`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`"table""name"`))
		})
		It("replaces backslashes with pairs of backslashes", func() {
			name := `table\name`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`"table\\name"`))
		})
		It("properly escapes capital letters", func() {
			names := []string{"Relationname", "TABLENAME", "TaBlEnAmE"}
			expected := []string{`"Relationname"`, `"TABLENAME"`, `"TaBlEnAmE"`}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes shell-significant special characters", func() {
			special := `.,!$/` + "`"
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes whitespace", func() {
			names := []string{"table name", "table\tname", "table\nname"}
			expected := []string{`"table name"`, "\"table\tname\"", "\"table\nname\""}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes all other punctuation", func() {
			special := `'~@#$%^&*()-+[]{}><|;:?`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes Unicode characters", func() {
			special := `Ää表`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
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
		alphabeticalAFoo := backup.Relation{1, 0, "otherschema", "foo", []string{}}
		alphabeticalABar := backup.Relation{1, 0, "otherschema", "bar", []string{}}
		schemaOther := backup.Schema{2, "otherschema"}
		alphabeticalBFoo := backup.Relation{2, 0, "public", "foo", []string{}}
		alphabeticalBBar := backup.Relation{2, 0, "public", "bar", []string{}}
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
	Describe("SortViews", func() {
		It("sorts by dependencies", func() {
			views := []backup.QueryViewDefinition{
				{SchemaName: "public", ViewName: "view1", DependsUpon: []string{"public.view2"}},
				{SchemaName: "public", ViewName: "view2", DependsUpon: []string{}},
				{SchemaName: "public", ViewName: "view3", DependsUpon: []string{"public.view1"}},
			}

			backup.SortViews(views)

			Expect(views[0].ToString()).To(Equal("public.view2"))
			Expect(views[1].ToString()).To(Equal("public.view1"))
			Expect(views[2].ToString()).To(Equal("public.view3"))
		})
	})
	Describe("SortRelations", func() {
		It("sorts by dependencies", func() {
			relations := []backup.Relation{
				{SchemaName: "public", RelationName: "relation1", DependsUpon: []string{"public.relation2"}},
				{SchemaName: "public", RelationName: "relation2", DependsUpon: []string{}},
				{SchemaName: "public", RelationName: "relation3", DependsUpon: []string{"public.relation1"}},
			}

			backup.SortRelations(relations)

			Expect(relations[0].ToString()).To(Equal("public.relation2"))
			Expect(relations[1].ToString()).To(Equal("public.relation1"))
			Expect(relations[2].ToString()).To(Equal("public.relation3"))
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

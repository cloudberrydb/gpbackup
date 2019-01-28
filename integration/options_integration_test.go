package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Options Integration", func() {
	Describe("QuoteTablesNames", func() {
		It("returns unchanged when fqn has no special characters", func() {
			tableList := []string{
				`foo.bar`,
			}

			resultFQNs, err := options.QuoteTableNames(connectionPool, tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(tableList).To(Equal(resultFQNs))
		})
		It("adds quote to single fqn when fqn has special characters", func() {
			tableList := []string{
				`FOO.bar`,
			}
			expected := []string{
				`"FOO".bar`,
			}

			resultFQNs, err := options.QuoteTableNames(connectionPool, tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(expected).To(Equal(resultFQNs))
		})
		It("adds quotes as necessary to multiple entries", func() {
			tableList := []string{
				`foo.BAR`,
				`FOO.BAR`,
				`bim.2`,
				`public.foo_bar`,
				`foo ~#$%^&*()_-+[]{}><\|;:/?!.bar`,
				"tab\t.bar", // important to use double quotes to allow \t to become tab
				"tab\n.bar", // important to use double quotes to allow \t to become tab
			}
			expected := []string{
				`foo."BAR"`,
				`"FOO"."BAR"`,
				`bim."2"`,
				`public.foo_bar`, // underscore is NOT special
				`"foo ~#$%^&*()_-+[]{}><\|;:/?!".bar`,
				"\"tab\t\".bar",
				"\"tab\n\".bar",
			}

			resultFQNs, err := options.QuoteTableNames(connectionPool, tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(expected).To(Equal(resultFQNs))
		})
		It("add quotes as necessary for embedded single, double quotes", func() {
			tableList := []string{
				`single''quote.bar`,    // escape single-quote with another single-quote
				`double""quote.bar`,    // escape double-quote with another double-quote
				`doublequote.bar""baz`, // escape double-quote with another double-quote
			}
			expected := []string{
				`"single'quote".bar`,
				`"double""""quote".bar`,
				`doublequote."bar""""baz"`,
			}

			resultFQNs, err := options.QuoteTableNames(connectionPool, tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(expected).To(Equal(resultFQNs))
		})
	})
	Describe("ValidateFilterTables", func() {
		It("validates special chars", func() {
			createSpecialCharacterTables := `
-- special chars
CREATE TABLE public."FOObar" (i int);
CREATE TABLE public."BAR" (i int);
CREATE SCHEMA "CAPschema";
CREATE TABLE "CAPschema"."BAR" (i int);
CREATE TABLE "CAPschema".baz (i int);
CREATE TABLE public.foo_bar (i int);
CREATE TABLE public."foo ~#$%^&*()_-+[]{}><\|;:/?!bar" (i int);
-- special chars: embedded tab char
CREATE TABLE public."tab	bar" (i int);
-- special chars: embedded newline char
CREATE TABLE public."newline
bar" (i int);
`
			dropSpecialCharacterTables := `
-- special chars
DROP TABLE public."FOObar";
DROP TABLE public."BAR";
DROP SCHEMA "CAPschema" cascade;
DROP TABLE public.foo_bar;
DROP TABLE public."foo ~#$%^&*()_-+[]{}><\|;:/?!bar";
-- special chars: embedded tab char
DROP TABLE public."tab	bar";
-- special chars: embedded newline char
DROP TABLE public."newline
bar";
`
			testhelper.AssertQueryRuns(connectionPool, createSpecialCharacterTables)
			defer testhelper.AssertQueryRuns(connectionPool, dropSpecialCharacterTables)

			tableList := []string{
				`public.BAR`,
				`CAPschema.BAR`,
				`CAPschema.baz`,
				`public.foo_bar`,
				`public.foo ~#$%^&*()_-+[]{}><\|;:/?!bar`,
				"public.tab\tbar",     // important to use double quotes to allow \t to become tab
				"public.newline\nbar", // important to use double quotes to allow \n to become newline
			}

			backup.DBValidate(connectionPool, tableList, false)
		})
	})
})

package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetTextSearchParsers", func() {
		It("returns a text search parser without a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: ""}

			Expect(len(parsers)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser with a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}

			Expect(len(parsers)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
	})
	Describe("GetTextSearchTemplates", func() {
		It("returns a text search template without an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "", LexizeFunc: "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template with an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(INIT = dsimple_init, LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
	})
	Describe("GetTextSearchDictionaries", func() {
		It("returns a text search dictionary with init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
			Expect(len(dictionaries)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary without init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary (TEMPLATE = 'simple');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.simple", InitOption: ""}
			Expect(len(dictionaries)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
	})
	Describe("GetTextSearchConfigurations", func() {
		It("returns a text search configuration without an init function", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration (PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(len(configurations)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with an init function", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration ( PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(len(configurations)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with mappings", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration ( PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")

			testutils.AssertQueryRuns(connection, "ALTER TEXT SEARCH CONFIGURATION testconfiguration ADD MAPPING FOR uint WITH simple;")
			testutils.AssertQueryRuns(connection, "ALTER TEXT SEARCH CONFIGURATION testconfiguration ADD MAPPING FOR asciiword WITH danish_stem;")

			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
			expectedConfiguration.TokenToDicts = map[string][]string{"uint": {"simple"}, "asciiword": {"danish_stem"}}

			Expect(len(configurations)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
	})
})

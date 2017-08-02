package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		testutils.SetupTestLogger()
	})
	Describe("GetTextSearchParsers", func() {
		It("returns a text search parser without a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{1, "public", "testparser", "prsd_start", "prsd_nexttoken", "prsd_end", "prsd_lextype", ""}

			Expect(len(parsers)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser with a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{1, "public", "testparser", "prsd_start", "prsd_nexttoken", "prsd_end", "prsd_lextype", "prsd_headline"}

			Expect(len(parsers)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
	})
	Describe("GetTextSearchTemplates", func() {
		It("returns a text search template without an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{1, "public", "testtemplate", "", "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template with an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(INIT = dsimple_init, LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{1, "public", "testtemplate", "dsimple_init", "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
	})
	Describe("GetTextSearchDictionaries", func() {
		It("returns a text search dictionary with init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{1, "public", "testdictionary", "pg_catalog.snowball", "language = 'russian', stopwords = 'russian'"}

			Expect(len(dictionaries)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary without init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary (TEMPLATE = 'simple');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{1, "public", "testdictionary", "pg_catalog.simple", ""}

			Expect(len(dictionaries)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
	})
})

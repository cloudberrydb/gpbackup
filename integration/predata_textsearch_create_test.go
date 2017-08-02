package integration

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		It("creates a basic text search parser", func() {
			parsers := []backup.TextSearchParser{{0, "public", "testparser", "prsd_start", "prsd_nexttoken", "prsd_end", "prsd_lextype", "prsd_headline"}}

			backup.PrintCreateTextSearchParserStatements(buffer, parsers, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")

			resultParsers := backup.GetTextSearchParsers(connection)

			Expect(len(resultParsers)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&parsers[0], &resultParsers[0], "Oid")
		})
		It("creates a basic text search parser with a comment", func() {
			parsers := []backup.TextSearchParser{{1, "public", "testparser", "prsd_start", "prsd_nexttoken", "prsd_end", "prsd_lextype", "prsd_headline"}}
			parserMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH PARSER", false, false, true)
			parserMetadata := parserMetadataMap[1]

			backup.PrintCreateTextSearchParserStatements(buffer, parsers, parserMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")

			resultParsers := backup.GetTextSearchParsers(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TSParserParams)

			Expect(len(resultParsers)).To(Equal(1))
			oid := backup.OidFromObjectName(connection, "public", "testparser", backup.TSParserParams)
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&parsers[0], &resultParsers[0], "Oid")
			testutils.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatements", func() {
		It("creates a basic text search template", func() {
			templates := []backup.TextSearchTemplate{{0, "public", "testtemplate", "dsimple_init", "dsimple_lexize"}}

			backup.PrintCreateTextSearchTemplateStatements(buffer, templates, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connection)

			Expect(len(resultTemplates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&templates[0], &resultTemplates[0], "Oid")
		})
		It("creates a basic text search template with a comment", func() {
			templates := []backup.TextSearchTemplate{{1, "public", "testtemplate", "dsimple_init", "dsimple_lexize"}}
			templateMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH TEMPLATE", false, false, true)
			templateMetadata := templateMetadataMap[1]

			backup.PrintCreateTextSearchTemplateStatements(buffer, templates, templateMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TSTemplateParams)

			Expect(len(resultTemplates)).To(Equal(1))
			oid := backup.OidFromObjectName(connection, "public", "testtemplate", backup.TSTemplateParams)
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&templates[0], &resultTemplates[0], "Oid")
			testutils.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatements", func() {
		It("creates a basic text search dictionary", func() {
			dictionaries := []backup.TextSearchDictionary{{0, "public", "testdictionary", "pg_catalog.snowball", "language = 'russian', stopwords = 'russian'"}}

			backup.PrintCreateTextSearchDictionaryStatements(buffer, dictionaries, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connection)

			Expect(len(resultDictionaries)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&dictionaries[0], &resultDictionaries[0], "Oid")
		})
		It("creates a basic text search dictionary with a comment and owner", func() {
			dictionaries := []backup.TextSearchDictionary{{1, "public", "testdictionary", "pg_catalog.snowball", "language = 'russian', stopwords = 'russian'"}}
			dictionaryMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH DICTIONARY", false, true, true)
			dictionaryMetadata := dictionaryMetadataMap[1]

			backup.PrintCreateTextSearchDictionaryStatements(buffer, dictionaries, dictionaryMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TSDictionaryParams)

			Expect(len(resultDictionaries)).To(Equal(1))
			oid := backup.OidFromObjectName(connection, "public", "testdictionary", backup.TSDictionaryParams)
			resultMetadata := resultMetadataMap[oid]
			testutils.ExpectStructsToMatchExcluding(&dictionaries[0], &resultDictionaries[0], "Oid")
			testutils.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
		})
	})
})

package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata_textsearch tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		It("prints a basic text search parser", func() {
			parsers := []backup.TextSearchParser{{0, "public", "testparser", "start_func", "token_func", "end_func", "lextypes_func", ""}}
			backup.PrintCreateTextSearchParserStatements(buffer, parsers, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func
)`)
		})
		It("prints a text search parser with a headline", func() {
			parsers := []backup.TextSearchParser{{0, "public", "testparser", "start_func", "token_func", "end_func", "lextypes_func", "headline_func"}}
			backup.PrintCreateTextSearchParserStatements(buffer, parsers, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func,
	HEADLINE = headline_func
)`)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatements", func() {
		It("prints a basic text search template", func() {
			templates := []backup.TextSearchTemplate{{0, "public", "testtemplate", "dsimple_init", "dsimple_lexize"}}
			backup.PrintCreateTextSearchTemplateStatements(buffer, templates, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH TEMPLATE public.testtemplate (
	INIT = dsimple_init,
	LEXIZE = dsimple_lexize
)`)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatements", func() {
		It("prints a basic text search dictionary", func() {
			dictionaries := []backup.TextSearchDictionary{{0, "public", "testdictionary", "testschema.snowball", "language = 'russian', stopwords = 'russian'"}}
			backup.PrintCreateTextSearchDictionaryStatements(buffer, dictionaries, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH DICTIONARY public.testdictionary (
	TEMPLATE = testschema.snowball,
	language = 'russian', stopwords = 'russian'
)`)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatements", func() {
		It("prints a basic text search configuration", func() {
			configurations := []backup.TextSearchConfiguration{{0, "public", "testconfiguration", `pg_catalog."default"`, map[string][]string{}}}
			backup.PrintCreateTextSearchConfigurationStatements(buffer, configurations, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
)`)
		})
		It("prints a text search configuration with multiple mappings", func() {
			tokenToDicts := map[string][]string{"int": {"simple", "english_stem"}, "asciiword": {"english_stem"}}
			configurations := []backup.TextSearchConfiguration{{0, "public", "testconfiguration", `pg_catalog."default"`, tokenToDicts}}
			backup.PrintCreateTextSearchConfigurationStatements(buffer, configurations, backup.MetadataMap{})
			testutils.ExpectRegexp(buffer, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
);

ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "asciiword" WITH english_stem;

ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "int" WITH simple, english_stem;`)
		})
	})
})

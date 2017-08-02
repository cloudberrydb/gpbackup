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
})

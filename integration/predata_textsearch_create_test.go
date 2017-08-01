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
})

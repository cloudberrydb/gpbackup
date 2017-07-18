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
	Describe("PrintCreateIndexStatements", func() {
		var (
			indexNameMap     map[string]bool
			indexMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			indexNameMap = map[string]bool{}
			indexMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic index", func() {
			indexes := []backup.QuerySimpleDefinition{
				{0, "index1", "public", "testtable", "CREATE INDEX index1 ON testtable USING btree (i)"},
			}
			backup.PrintCreateIndexStatements(buffer, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultIndexes := backup.GetIndexDefinitions(connection, indexNameMap)
			Expect(len(resultIndexes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []backup.QuerySimpleDefinition{
				{1, "index1", "public", "testtable", "CREATE INDEX index1 ON testtable USING btree (i)"},
			}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true)
			indexMetadata := indexMetadataMap[1]
			backup.PrintCreateIndexStatements(buffer, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			indexes[0].Oid = backup.OidFromObjectName(connection, "index1", "relname", "pg_class")
			resultIndexes := backup.GetIndexDefinitions(connection, indexNameMap)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "indexrelid", "pg_class", "pg_index")
			resultMetadata := resultMetadataMap[indexes[0].Oid]
			Expect(len(resultIndexes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
			testutils.ExpectStructsToMatch(&resultMetadata, &indexMetadata)
		})
	})
	Describe("PrintCreateRuleStatements", func() {
		var (
			ruleMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			ruleMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic rule", func() {
			rules := []backup.QuerySimpleDefinition{
				{0, "update_notify", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"},
			}
			backup.PrintCreateRuleStatements(buffer, rules, ruleMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultRules := backup.GetRuleDefinitions(connection)
			Expect(len(resultRules)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{
				{1, "update_notify", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"},
			}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			ruleMetadata := ruleMetadataMap[1]
			backup.PrintCreateRuleStatements(buffer, rules, ruleMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE TABLE testtable(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testtable")

			testutils.AssertQueryRuns(connection, buffer.String())

			rules[0].Oid = backup.OidFromObjectName(connection, "update_notify", "rulename", "pg_rewrite")
			resultRules := backup.GetRuleDefinitions(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, "", "oid", "pg_rewrite", "pg_rewrite")
			resultMetadata := resultMetadataMap[rules[0].Oid]
			Expect(len(resultRules)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
			testutils.ExpectStructsToMatch(&resultMetadata, &ruleMetadata)
		})
	})
})

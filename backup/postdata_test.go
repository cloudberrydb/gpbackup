package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/postdata tests", func() {
	var buffer *gbytes.Buffer
	BeforeEach(func() {
		buffer = gbytes.NewBuffer()
		testutils.SetupTestLogger()
	})
	Context("PrintCreateIndexStatements", func() {
		It("can print a basic index", func() {
			indexes := []backup.QuerySimpleDefinition{{1, "testindex", "public", "testtable", "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			emptyMetadataMap := utils.MetadataMap{}
			backup.PrintCreateIndexStatements(buffer, indexes, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE INDEX testindex ON public.testtable USING btree(i);`)
		})
		It("can print an index with a comment", func() {
			indexes := []backup.QuerySimpleDefinition{{1, "testindex", "public", "testtable", "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			indexMetadataMap := utils.MetadataMap{1: {Comment: "This is an index comment."}}
			backup.PrintCreateIndexStatements(buffer, indexes, indexMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE INDEX testindex ON public.testtable USING btree(i);

COMMENT ON INDEX testindex IS 'This is an index comment.';`)
		})
	})
	Context("PrintCreateRuleStatements", func() {
		It("can print a basic rule", func() {
			rules := []backup.QuerySimpleDefinition{{1, "testrule", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			emptyMetadataMap := utils.MetadataMap{}
			backup.PrintCreateRuleStatements(buffer, rules, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;`)
		})
		It("can print a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{{1, "testrule", "public", "testtable", "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			ruleMetadataMap := utils.MetadataMap{1: {Comment: "This is a rule comment."}}
			backup.PrintCreateRuleStatements(buffer, rules, ruleMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;

COMMENT ON RULE testrule ON public.testtable IS 'This is a rule comment.';`)
		})
	})
	Context("PrintCreateTriggerStatements", func() {
		It("can print a basic trigger", func() {
			triggers := []backup.QuerySimpleDefinition{{1, "testtrigger", "public", "testtable", "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			emptyMetadataMap := utils.MetadataMap{}
			backup.PrintCreateTriggerStatements(buffer, triggers, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();`)
		})
		It("can print a trigger with a comment", func() {
			triggers := []backup.QuerySimpleDefinition{{1, "testtrigger", "public", "testtable", "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			triggerMetadataMap := utils.MetadataMap{1: {Comment: "This is a trigger comment."}}
			backup.PrintCreateTriggerStatements(buffer, triggers, triggerMetadataMap)
			testutils.ExpectRegexp(buffer, `

CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();

COMMENT ON TRIGGER testtrigger ON public.testtable IS 'This is a trigger comment.';`)
		})
	})
})

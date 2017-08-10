package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("ConstructImplicitIndexNames", func() {
		It("returns an empty map if there are no implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			indexNameMap := backup.ConstructImplicitIndexNames(connection)

			Expect(len(indexNameMap)).To(Equal(0))
		})
		It("returns a map of all implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int UNIQUE)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			indexNameMap := backup.ConstructImplicitIndexNames(connection)

			Expect(len(indexNameMap)).To(Equal(1))
			Expect(indexNameMap["public.simple_table_i_key"]).To(BeTrue())
		})
	})
	Describe("GetIndexDefinitions", func() {
		var indexNameMap map[string]bool
		BeforeEach(func() {
			indexNameMap = make(map[string]bool, 0)
		})
		It("returns no slice when no index exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			results := backup.GetIndexes(connection, indexNameMap)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx2 ON simple_table(j)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx2")

			index1 := backup.QuerySimpleDefinition{0, "simple_table_idx1", "public", "simple_table", "",
				"CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)"}
			index2 := backup.QuerySimpleDefinition{1, "simple_table_idx2", "public", "simple_table", "",
				"CREATE INDEX simple_table_idx2 ON simple_table USING btree (j)"}

			results := backup.GetIndexes(connection, indexNameMap)

			Expect(len(results)).To(Equal(2))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx1", backup.TYPE_INDEX)
			results[1].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx2", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&index2, &results[1], "Oid")
		})
		It("returns a slice of multiple indexes, excluding implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int UNIQUE, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx2 ON simple_table(j)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx2")
			indexNameMap["public.simple_table_i_key"] = true

			index1 := backup.QuerySimpleDefinition{0, "simple_table_idx1", "public", "simple_table", "",
				"CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)"}
			index2 := backup.QuerySimpleDefinition{1, "simple_table_idx2", "public", "simple_table", "",
				"CREATE INDEX simple_table_idx2 ON simple_table USING btree (j)"}

			results := backup.GetIndexes(connection, indexNameMap)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&index2, &results[1], "Oid")
		})
		It("returns a slice of indexes for only partition parent tables", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part (id int, date date, amt decimal(10,2)) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);
`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part")
			testutils.AssertQueryRuns(connection, "CREATE INDEX part_idx ON part(id)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX part_idx")

			index1 := backup.QuerySimpleDefinition{0, "part_idx", "public", "part", "",
				"CREATE INDEX part_idx ON part USING btree (id)"}

			results := backup.GetIndexes(connection, indexNameMap)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice containing an index in a non-default tablespace", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_filespace")
			defer testutils.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx ON simple_table(i) TABLESPACE test_tablespace")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx")

			index1 := backup.QuerySimpleDefinition{0, "simple_table_idx", "public", "simple_table", "test_tablespace",
				"CREATE INDEX simple_table_idx ON simple_table USING btree (i)"}

			results := backup.GetIndexes(connection, indexNameMap)

			Expect(len(results)).To(Equal(1))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
	})
	Describe("GetRuleDefinitions", func() {
		It("returns no slice when no rule exists", func() {
			results := backup.GetRules(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple rules", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE rule_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE rule_table2(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rule_table2")
			testutils.AssertQueryRuns(connection, "CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table2 DEFAULT VALUES")
			defer testutils.AssertQueryRuns(connection, "DROP RULE double_insert ON rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE RULE update_notify AS ON UPDATE TO rule_table1 DO NOTIFY rule_table1")
			defer testutils.AssertQueryRuns(connection, "DROP RULE update_notify ON rule_table1")
			testutils.AssertQueryRuns(connection, "COMMENT ON RULE update_notify ON rule_table1 IS 'This is a rule comment.'")

			rule1 := backup.QuerySimpleDefinition{0, "double_insert", "public", "rule_table1", "",
				"CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table2 DEFAULT VALUES;"}
			rule2 := backup.QuerySimpleDefinition{1, "update_notify", "public", "rule_table1", "",
				"CREATE RULE update_notify AS ON UPDATE TO rule_table1 DO NOTIFY rule_table1;"}

			results := backup.GetRules(connection)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&rule2, &results[1], "Oid")
		})
	})
	Describe("GetTriggerDefinitions", func() {
		It("returns no slice when no trigger exists", func() {
			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple triggers", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table2(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table2")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table1 ON trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table2 ON trigger_table2")
			testutils.AssertQueryRuns(connection, "COMMENT ON TRIGGER sync_trigger_table2 ON trigger_table2 IS 'This is a trigger comment.'")

			trigger1 := backup.QuerySimpleDefinition{0, "sync_trigger_table1", "public", "trigger_table1", "",
				"CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()",
			}
			trigger2 := backup.QuerySimpleDefinition{1, "sync_trigger_table2", "public", "trigger_table2", "",
				"CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()",
			}

			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&trigger2, &results[1], "Oid")
		})
		It("does not include constraint triggers", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table1(i int PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table2(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table2")
			testutils.AssertQueryRuns(connection, "ALTER TABLE trigger_table2 ADD CONSTRAINT fkc FOREIGN KEY (j) REFERENCES trigger_table1 (i) ON UPDATE RESTRICT ON DELETE RESTRICT")

			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(0))
		})
	})
})

package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/global objects tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintSessionGUCs", func() {
		It("prints session GUCs", func() {
			gucs := backup.QuerySessionGUCs{"UTF8", "on", "false"}

			backup.PrintSessionGUCs(buffer, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		dbname := "testdb"
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO 'pg_catalog, public'"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true,blocksize=32768'"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';
ALTER DATABASE testdb SET search_path TO 'pg_catalog, public';
ALTER DATABASE testdb SET gp_default_storage_options TO 'appendonly=true,blocksize=32768';`)
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		It("prints resource queues", func() {
			someQueue := backup.QueryResourceQueue{"some_queue", 1, "-1.00", false, "0.00", "medium", "-1", ""}
			maxCostQueue := backup.QueryResourceQueue{"someMaxCostQueue", -1, "99.9", true, "0.00", "medium", "-1", ""}
			resQueues := []backup.QueryResourceQueue{someQueue, maxCostQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE some_queue WITH (ACTIVE_STATEMENTS=1);

CREATE RESOURCE QUEUE "someMaxCostQueue" WITH (MAX_COST=99.9, COST_OVERCOMMIT=TRUE);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			someActiveMaxCostQueue := backup.QueryResourceQueue{"someActiveMaxCostQueue", 5, "62.03", false, "0.00", "medium", "-1", ""}
			resQueues := []backup.QueryResourceQueue{someActiveMaxCostQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "someActiveMaxCostQueue" WITH (ACTIVE_STATEMENTS=5, MAX_COST=62.03);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			everythingQueue := backup.QueryResourceQueue{"everythingQueue", 7, "32.80", true, "1.34", "low", "2GB", ""}
			resQueues := []backup.QueryResourceQueue{everythingQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "everythingQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=32.80, COST_OVERCOMMIT=TRUE, MIN_COST=1.34, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
		})
		It("prints a resource queue with a comment", func() {
			commentQueue := backup.QueryResourceQueue{"commentQueue", 1, "-1.00", false, "0.00", "medium", "-1", "this is a comment on a resource queue"}
			resQueues := []backup.QueryResourceQueue{commentQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "commentQueue" WITH (ACTIVE_STATEMENTS=1);

COMMENT ON RESOURCE QUEUE "commentQueue" IS 'this is a comment on a resource queue'`)
		})
		It("prints ALTER statement for pg_default resource queue", func() {
			pg_default := backup.QueryResourceQueue{"pg_default", 1, "-1.00", false, "0.00", "medium", "-1", ""}
			resQueues := []backup.QueryResourceQueue{pg_default}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues)
			testutils.ExpectRegexp(buffer, `ALTER RESOURCE QUEUE pg_default WITH (ACTIVE_STATEMENTS=1);`)
		})
	})
})

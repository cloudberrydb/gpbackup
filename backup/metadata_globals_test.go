package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/metadata_globals tests", func() {
	var toc *utils.TOC
	var backupfile *utils.FileWithByteCount

	BeforeEach(func() {
		toc = &utils.TOC{}
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("PrintPredataSessionGUCs", func() {
		It("prints predata session GUCs", func() {
			gucs := backup.SessionGUCs{"UTF8", "on", "false"}

			backup.PrintPredataSessionGUCs(backupfile, toc, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})
	Describe("PrintPostdataSessionGUCs", func() {
		It("prints predata session GUCs", func() {
			gucs := backup.SessionGUCs{"UTF8", "on", "false"}

			backup.PrintPostdataSessionGUCs(backupfile, toc, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})
	Describe("PrintGlobalSessionGUCs", func() {
		It("prints predata session GUCs", func() {
			gucs := backup.SessionGUCs{"UTF8", "on", "false"}

			backup.PrintGlobalSessionGUCs(backupfile, toc, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})
	Describe("PrintCreateDatabaseStatement", func() {
		It("prints a basic CREATE DATABASE statement", func() {
			dbs := []backup.DatabaseName{{1, "testdb", "pg_default"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, toc, "testdb", dbs, emptyMetadataMap, false)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "testdb", "DATABASE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb;`)
		})
		It("prints a CREATE DATABASE statement with privileges, an owner, and a comment", func() {
			dbMetadataMap := testutils.DefaultMetadataMap("DATABASE", true, true, true)
			dbMetadata := dbMetadataMap[1]
			dbMetadata.Privileges[0].Create = false
			dbMetadataMap[1] = dbMetadata
			dbs := []backup.DatabaseName{{1, "testdb", "pg_default"}, {2, "otherdb", "pg_default"}}
			backup.PrintCreateDatabaseStatement(backupfile, toc, "testdb", dbs, dbMetadataMap, false)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb;

COMMENT ON DATABASE testdb IS 'This is a database comment.';


ALTER DATABASE testdb OWNER TO testrole;


REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT TEMPORARY,CONNECT ON DATABASE testdb TO testrole;`)
		})
		It("prints a CREATE DATABASE statement with privileges for testdb and only prints privileges for otherdb", func() {
			dbMetadataMap := backup.MetadataMap{
				1: backup.ObjectMetadata{
					Privileges: []backup.ACL{{Grantee: "testrole", TemporaryWithGrant: true, ConnectWithGrant: true}},
					Owner:      "testrole",
					Comment:    "This is a database comment."},
				2: backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", Create: true}}},
				3: backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", CreateWithGrant: true}}},
			}
			dbs := []backup.DatabaseName{{1, "testdb", "pg_default"}, {2, "otherdb", "pg_default"}, {3, "anotherdb", "pg_default"}}
			backup.PrintCreateDatabaseStatement(backupfile, toc, "testdb", dbs, dbMetadataMap, true)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb;

COMMENT ON DATABASE testdb IS 'This is a database comment.';


ALTER DATABASE testdb OWNER TO testrole;


REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT TEMPORARY,CONNECT ON DATABASE testdb TO testrole WITH GRANT OPTION;


REVOKE ALL ON DATABASE otherdb FROM PUBLIC;
GRANT CREATE ON DATABASE otherdb TO testrole;


REVOKE ALL ON DATABASE anotherdb FROM PUBLIC;
GRANT CREATE ON DATABASE anotherdb TO testrole WITH GRANT OPTION;`)
		})
		It("prints a CREATE DATABASE statement with a TABLESPACE", func() {
			dbs := []backup.DatabaseName{{1, "testdb", "test_tablespace"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, toc, "testdb", dbs, emptyMetadataMap, false)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb TABLESPACE test_tablespace;`)
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		dbname := "testdb"
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO 'pg_catalog, public'"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true,blocksize=32768'"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			backup.PrintDatabaseGUCs(backupfile, toc, gucs, dbname)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "testdb", "DATABASE GUC")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(backupfile, toc, gucs, dbname)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`ALTER DATABASE testdb SET default_with_oids TO 'true';`,
				`ALTER DATABASE testdb SET search_path TO 'pg_catalog, public';`,
				`ALTER DATABASE testdb SET gp_default_storage_options TO 'appendonly=true,blocksize=32768';`)
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		var emptyResQueueMetadata = map[uint32]backup.ObjectMetadata{}
		It("prints resource queues", func() {
			someQueue := backup.ResourceQueue{1, "some_queue", 1, "-1.00", false, "0.00", "medium", "-1"}
			maxCostQueue := backup.ResourceQueue{1, "someMaxCostQueue", -1, "99.9", true, "0.00", "medium", "-1"}
			resQueues := []backup.ResourceQueue{someQueue, maxCostQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "some_queue", "RESOURCE QUEUE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`CREATE RESOURCE QUEUE some_queue WITH (ACTIVE_STATEMENTS=1);`,
				`CREATE RESOURCE QUEUE "someMaxCostQueue" WITH (MAX_COST=99.9, COST_OVERCOMMIT=TRUE);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			someActiveMaxCostQueue := backup.ResourceQueue{1, "someActiveMaxCostQueue", 5, "62.03", false, "0.00", "medium", "-1"}
			resQueues := []backup.ResourceQueue{someActiveMaxCostQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "someActiveMaxCostQueue" WITH (ACTIVE_STATEMENTS=5, MAX_COST=62.03);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			everythingQueue := backup.ResourceQueue{1, "everythingQueue", 7, "32.80", true, "1.34", "low", "2GB"}
			resQueues := []backup.ResourceQueue{everythingQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "everythingQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=32.80, COST_OVERCOMMIT=TRUE, MIN_COST=1.34, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
		})
		It("prints a resource queue with a comment", func() {
			commentQueue := backup.ResourceQueue{1, "commentQueue", 1, "-1.00", false, "0.00", "medium", "-1"}
			resQueues := []backup.ResourceQueue{commentQueue}
			resQueueMetadata := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)

			backup.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, resQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "commentQueue" WITH (ACTIVE_STATEMENTS=1);

COMMENT ON RESOURCE QUEUE "commentQueue" IS 'This is a resource queue comment.';`)
		})
		It("prints ALTER statement for pg_default resource queue", func() {
			pg_default := backup.ResourceQueue{1, "pg_default", 1, "-1.00", false, "0.00", "medium", "-1"}
			resQueues := []backup.ResourceQueue{pg_default}

			backup.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `ALTER RESOURCE QUEUE pg_default WITH (ACTIVE_STATEMENTS=1);`)
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		testrole1 := backup.Role{
			Oid:             1,
			Name:            "testrole1",
			Super:           false,
			Inherit:         false,
			CreateRole:      false,
			CreateDB:        false,
			CanLogin:        false,
			ConnectionLimit: -1,
			Password:        "",
			ValidUntil:      "",
			ResQueue:        "pg_default",
			Createrexthttp:  false,
			Createrextgpfd:  false,
			Createwextgpfd:  false,
			Createrexthdfs:  false,
			Createwexthdfs:  false,
			TimeConstraints: []backup.TimeConstraint{},
		}

		testrole2 := backup.Role{
			Oid:             1,
			Name:            "testRole2",
			Super:           true,
			Inherit:         true,
			CreateRole:      true,
			CreateDB:        true,
			CanLogin:        true,
			ConnectionLimit: 4,
			Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
			ValidUntil:      "2099-01-01 00:00:00-08",
			ResQueue:        "testQueue",
			Createrexthttp:  true,
			Createrextgpfd:  true,
			Createwextgpfd:  true,
			Createrexthdfs:  true,
			Createwexthdfs:  true,
			TimeConstraints: []backup.TimeConstraint{
				{
					StartDay:  0,
					StartTime: "13:30:00",
					EndDay:    3,
					EndTime:   "14:30:00",
				}, {
					StartDay:  5,
					StartTime: "00:00:00",
					EndDay:    5,
					EndTime:   "24:00:00",
				},
			},
		}
		It("prints basic role", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{testrole1}, roleMetadataMap)

			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "testrole1", "ROLE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default;

COMMENT ON ROLE testrole1 IS 'This is a role comment.';`)
		})
		It("prints roles with non-defaults", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{testrole2}, roleMetadataMap)

			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';

COMMENT ON ROLE "testRole2" IS 'This is a role comment.';`)
		})
		It("prints multiple roles", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{testrole1, testrole2}, emptyMetadataMap)

			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default;`,
				`CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';`)
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		roleWith := backup.RoleMember{"group", "rolewith", "grantor", true}
		roleWithout := backup.RoleMember{"group", "rolewithout", "grantor", false}
		It("prints a role without ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(backupfile, toc, []backup.RoleMember{roleWithout})
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "rolewithout", "ROLE GRANT")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `GRANT group TO rolewithout GRANTED BY grantor;`)
		})
		It("prints a role WITH ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(backupfile, toc, []backup.RoleMember{roleWith})
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`)
		})
		It("prints multiple roles", func() {
			backup.PrintRoleMembershipStatements(backupfile, toc, []backup.RoleMember{roleWith, roleWithout})
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`,
				`GRANT group TO rolewithout GRANTED BY grantor;`)
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		expectedTablespace := backup.Tablespace{1, "test_tablespace", "test_filespace"}
		It("prints a basic tablespace", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, toc, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "test_tablespace", "TABLESPACE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`)
		})
		It("prints a tablespace with privileges, an owner, and a comment", func() {
			tablespaceMetadataMap := testutils.DefaultMetadataMap("TABLESPACE", true, true, true)
			backup.PrintCreateTablespaceStatements(backupfile, toc, []backup.Tablespace{expectedTablespace}, tablespaceMetadataMap)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;

COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.';


ALTER TABLESPACE test_tablespace OWNER TO testrole;


REVOKE ALL ON TABLESPACE test_tablespace FROM PUBLIC;
REVOKE ALL ON TABLESPACE test_tablespace FROM testrole;
GRANT ALL ON TABLESPACE test_tablespace TO testrole;`)
		})
	})
})

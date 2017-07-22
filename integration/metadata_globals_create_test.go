package integration

import (
	"bytes"
	"regexp"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup integration create statement tests", func() {
	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
		testutils.SetupTestLogger()
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		It("creates a basic resource queue with a comment", func() {
			basicQueue := backup.QueryResourceQueue{1, "basicQueue", -1, "32.80", false, "0.00", "medium", "-1"}
			resQueueMetadataMap := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)
			resQueueMetadata := resQueueMetadataMap[1]

			backup.PrintCreateResourceQueueStatements(buffer, []backup.QueryResourceQueue{basicQueue}, resQueueMetadataMap)

			// CREATE RESOURCE QUEUE statements can not be part of a multi-command statement, so
			// feed the CREATE RESOURCE QUEUE and COMMENT ON statements separately.
			hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 2)
			testutils.AssertQueryRuns(connection, hunks[0])
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "basicQueue"`)
			testutils.AssertQueryRuns(connection, hunks[1])

			resultResourceQueues := backup.GetResourceQueues(connection)
			resQueueOid := backup.OidFromObjectName(connection, "basicQueue", backup.ResQueueParams)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.ResQueueParams)
			resultMetadata := resultMetadataMap[resQueueOid]
			testutils.ExpectStructsToMatch(&resultMetadata, &resQueueMetadata)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == "basicQueue" {
					testutils.ExpectStructsToMatchExcluding(&basicQueue, &resultQueue, "Oid")
					return
				}
			}
		})
		It("creates a resource queue with all attributes", func() {
			everythingQueue := backup.QueryResourceQueue{1, "everythingQueue", 7, "32.80", true, "22.80", "low", "2GB"}
			emptyMetadataMap := map[uint32]backup.ObjectMetadata{}

			backup.PrintCreateResourceQueueStatements(buffer, []backup.QueryResourceQueue{everythingQueue}, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "everythingQueue"`)

			resultResourceQueues := backup.GetResourceQueues(connection)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == "everythingQueue" {
					testutils.ExpectStructsToMatchExcluding(&everythingQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("didn't find everythingQueue :(")
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		It("creates a basic role ", func() {
			role1 := backup.QueryRole{
				Oid:             0,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "",
				Comment:         "",
				ResQueue:        "pg_default",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}

			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{role1})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP ROLE "role1"`)
			role1.Oid = backup.OidFromObjectName(connection, "role1", backup.RoleParams)

			resultRoles := backup.GetRoles(connection)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					testutils.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a role with all attributes", func() {
			role1 := backup.QueryRole{
				Oid:             0,
				Name:            "role1",
				Super:           false,
				Inherit:         true,
				CreateRole:      true,
				CreateDB:        true,
				CanLogin:        true,
				ConnectionLimit: 4,
				Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
				ValidUntil:      "2099-01-01 08:00:00-00",
				Comment:         "this is a role comment",
				ResQueue:        "pg_default",
				Createrexthttp:  true,
				Createrextgpfd:  true,
				Createwextgpfd:  true,
				Createrexthdfs:  true,
				Createwexthdfs:  true,
				TimeConstraints: []backup.TimeConstraint{
					{
						Oid:       0,
						StartDay:  0,
						StartTime: "13:30:00",
						EndDay:    3,
						EndTime:   "14:30:00",
					}, {
						Oid:       0,
						StartDay:  5,
						StartTime: "00:00:00",
						EndDay:    5,
						EndTime:   "24:00:00",
					},
				},
			}

			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{role1})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, `DROP ROLE "role1"`)
			role1.Oid = backup.OidFromObjectName(connection, "role1", backup.RoleParams)

			resultRoles := backup.GetRoles(connection)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					testutils.ExpectStructsToMatchExcluding(&role1, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
})

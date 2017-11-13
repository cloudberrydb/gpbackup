package restore_test

import (
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/parallel tests", func() {
	commentStr := "-- This is a comment."
	createStr := "\nCREATE DATABASE foo;\n"
	gucStr := "\nSET fsync TO off;\n"
	var statements []utils.StatementWithType
	BeforeEach(func() {
		statements = []utils.StatementWithType{
			{ObjectType: "COMMENT", Statement: commentStr},
			{ObjectType: "DATABASE", Statement: createStr},
			{ObjectType: "SESSION GUCS", Statement: gucStr},
		}
	})
	Describe("Parallel statement execution functions", func() {
		Context("Serial execution", func() {
			BeforeEach(func() {
				restore.SetNumJobs(1)
			})
			Context("Dbconn.ExecuteAllStatements", func() {
				It("can execute all statements in the list serially", func() {
					mock.ExpectExec(commentStr).WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec(createStr).WillReturnResult(sqlmock.NewResult(1, 0))
					mock.ExpectExec(gucStr).WillReturnResult(sqlmock.NewResult(0, 1))
					restore.ExecuteAllStatements(statements, "", false)
				})
			})
			Context("Dbconn.ExecuteAllStatementsExcept", func() {
				It("can execute all statements in the list that are not of the specified object type serially", func() {
					mock.ExpectExec(commentStr).WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec(gucStr).WillReturnResult(sqlmock.NewResult(0, 1))
					restore.ExecuteAllStatementsExcept(statements, "", false, "DATABASE")
				})
			})
		})
		Context("Parallel execution", func() {
			BeforeEach(func() {
				restore.SetNumJobs(2)
				mock.MatchExpectationsInOrder(false)
			})
			AfterEach(func() {
				mock.MatchExpectationsInOrder(true)
			})
			Context("Dbconn.ExecuteAllStatements", func() {
				It("can execute all statements in the list in parallel", func() {
					mock.ExpectExec(commentStr).WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec(createStr).WillReturnResult(sqlmock.NewResult(1, 0))
					mock.ExpectExec(gucStr).WillReturnResult(sqlmock.NewResult(0, 1))
					restore.ExecuteAllStatements(statements, "", false)
				})
			})
			Context("Dbconn.ExecuteAllStatementsExcept", func() {
				It("can execute all statements in the list that are not of the specified object type in parallel", func() {
					mock.ExpectExec(commentStr).WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec(gucStr).WillReturnResult(sqlmock.NewResult(0, 1))
					restore.ExecuteAllStatementsExcept(statements, "", false, "DATABASE")
				})
			})
		})
	})
})

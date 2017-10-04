package utils_test

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/toc tests", func() {
	comment := utils.StatementWithType{"COMMENT", "-- This is a comment\n"}
	commentLen := uint64(len(comment.Statement))
	create := utils.StatementWithType{"DATABASE", "CREATE DATABASE some-database;\n"}
	createLen := uint64(len(create.Statement))
	role1 := utils.StatementWithType{"ROLE", "CREATE ROLE some-role1;\n"}
	role1Len := uint64(len(role1.Statement))
	role2 := utils.StatementWithType{"ROLE", "CREATE ROLE some-role2;\n"}
	role2Len := uint64(len(role2.Statement))
	Context("GetSqlStatementForObjectTypes", func() {
		var toc utils.TOC
		BeforeEach(func() {
			toc = utils.TOC{}
		})
		It("returns statement for a single object type", func() {
			toc.AddGlobalEntry("", "some-database", "DATABASE", commentLen, commentLen+createLen)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE")

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			toc.AddGlobalEntry("", "some-database", "DATABASE", commentLen, commentLen+createLen)
			toc.AddGlobalEntry("", "some-role1", "ROLE", commentLen+createLen, commentLen+createLen+role1Len)
			toc.AddGlobalEntry("", "some-role2", "ROLE", commentLen+createLen+role1Len, commentLen+createLen+role1Len+role2Len)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE", "ROLE")

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			toc.AddGlobalEntry("", "some-database", "DATABASE", commentLen, commentLen+createLen)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "TABLE")

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("GetAllSqlStatements", func() {
		var toc utils.TOC
		BeforeEach(func() {
			toc = utils.TOC{}
		})
		It("returns statement for a single object type", func() {
			toc.AddGlobalEntry("", "some-database", "DATABASE", 0, createLen)

			globalFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements(toc.GlobalEntries, globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			toc.AddGlobalEntry("", "some-database", "DATABASE", 0, createLen)
			toc.AddGlobalEntry("", "some-role1", "ROLE", createLen, createLen+role1Len)
			toc.AddGlobalEntry("", "some-role2", "ROLE", createLen+role1Len, createLen+role1Len+role2Len)

			globalFile := bytes.NewReader([]byte(create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetAllSQLStatements(toc.GlobalEntries, globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			globalFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements(toc.GlobalEntries, globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
})

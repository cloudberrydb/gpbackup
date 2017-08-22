package utils_test

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/toc tests", func() {
	Context("GetSqlStatementForObjectTypes", func() {
		var toc utils.TOC
		BeforeEach(func() {
			toc = utils.TOC{}
		})
		It("returns statement for a single object type", func() {
			comment := "-- This is a comment\n"
			create := "CREATE DATABASE some-database;"
			toc.AddGlobalEntry("", "some-database", "DATABASE", uint64(len(comment)), uint64(len(comment))+uint64(len(create)))

			globalFile := bytes.NewReader([]byte(comment + create))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE")

			Expect(statements).To(Equal([]string{"CREATE DATABASE some-database;"}))
		})
		It("returns statement for a multiple object types", func() {
			comment := "-- This is a comment\n"
			create := "CREATE DATABASE some-database;\n"
			role1 := "CREATE ROLE some-role1;\n"
			role2 := "CREATE ROLE some-role2;"
			toc.AddGlobalEntry("", "some-database", "DATABASE", uint64(len(comment)), uint64(len(comment)+len(create)))
			toc.AddGlobalEntry("", "some-role1", "ROLE", uint64(len(comment)+len(create)), uint64(len(comment)+len(create)+len(role1)))
			toc.AddGlobalEntry("", "some-role2", "ROLE", uint64(len(comment)+len(create)+len(role1)), uint64(len(comment)+len(create)+len(role1)+len(role2)))

			globalFile := bytes.NewReader([]byte(comment + create + role1 + role2))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE", "ROLE")

			Expect(statements).To(Equal([]string{"CREATE DATABASE some-database;\n", "CREATE ROLE some-role1;\n", "CREATE ROLE some-role2;"}))
		})
		It("returns empty statement when no object types are found", func() {
			comment := "-- This is a comment\n"
			create := "CREATE DATABASE some-database;\n"
			toc.AddGlobalEntry("", "some-database", "DATABASE", uint64(len(comment)), uint64(len(comment)+len(create)))

			globalFile := bytes.NewReader([]byte(comment + create))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "TABLE")

			Expect(statements).To(Equal([]string{}))
		})
	})
})

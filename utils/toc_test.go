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
	create := utils.StatementWithType{"DATABASE", "CREATE DATABASE somedatabase;\n"}
	createLen := uint64(len(create.Statement))
	role1 := utils.StatementWithType{"ROLE", "CREATE ROLE somerole1;\n"}
	role1Len := uint64(len(role1.Statement))
	role2 := utils.StatementWithType{"ROLE", "CREATE ROLE somerole2;\n"}
	role2Len := uint64(len(role2.Statement))
	var toc utils.TOC
	var globalCount *utils.FileWithByteCount
	BeforeEach(func() {
		toc = utils.TOC{}
		toc.InitializeEntryMap("buffer", "", "", "")
		globalCount = utils.NewFileWithByteCount(buffer)
		globalCount.Filename = "buffer"
	})
	Context("GetSqlStatementForObjectTypes", func() {
		It("returns statement for a single object type", func() {
			globalCount.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, globalCount)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE")

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			globalCount.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, globalCount)
			globalCount.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", commentLen+createLen, globalCount)
			globalCount.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", commentLen+createLen+role1Len, globalCount)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "DATABASE", "ROLE")

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			globalCount.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, globalCount)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes(toc.GlobalEntries, globalFile, "TABLE")

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("GetAllSqlStatements", func() {
		It("returns statement for a single object type", func() {
			globalCount.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", 0, globalCount)

			globalFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements(toc.GlobalEntries, globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			globalCount.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", 0, globalCount)
			globalCount.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", createLen, globalCount)
			globalCount.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", createLen+role1Len, globalCount)

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
	Context("SubstituteRedirectDatabaseInStatements", func() {
		var toc utils.TOC
		wrongCreate := utils.StatementWithType{"TABLE", "CREATE DATABASE somedatabase;\n"}
		gucs := utils.StatementWithType{"DATABASE GUC", "ALTER DATABASE somedatabase SET fsync TO off;\n"}
		metadata := utils.StatementWithType{"DATABASE METADATA", "ALTER DATABASE somedatabase OWNER TO testrole;\n"}
		oldSpecial := utils.StatementWithType{"DATABASE", `CREATE DATABASE "db-special-chär$";
`}

		BeforeEach(func() {
			toc = utils.TOC{}
		})
		It("can substitute a database name in a CREATE DATABASE statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase;\n"))
		})
		It("can substitute a database name in an ALTER DATABASE OWNER statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{metadata}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase OWNER TO testrole;\n"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("doesn't modify a statement of the wrong type", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{wrongCreate}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE somedatabase;\n"))
		})
		It("can substitute a database name if the old name contained special characters", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{oldSpecial}, "db-special-chär$", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase;\n"))
		})
		It("can substitute a database name if the new name contained special characters", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", "db-special-chär$")
			Expect(statements[0].Statement).To(Equal(`CREATE DATABASE "db-special-chär$";
`))
		})
	})
})

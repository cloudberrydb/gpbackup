package backup_test

import (
	"database/sql/driver"
	"gpbackup/backup"
	"gpbackup/testutils"
	"gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/queries tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})

	Describe("SelectString", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal("one"))
		})
		It("returns an empty string if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(""))
		})
		It("panics if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testutils.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			backup.SelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("SelectStringSlice", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a slice containing a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal("one"))
		})
		It("returns an empty slice if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple strings if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("one"))
			Expect(results[1]).To(Equal("two"))
		})
	})

	Describe("GetFunctionDefinitions", func() {
		header := []string{"nspname", "proname", "proretset", "functionbody", "binarypath", "arguments", "identargs", "resulttype",
			"provolatile", "proisstrict", "prosecdef", "proconfig", "procost", "prorows", "prodataaccess", "language", "comment", "owner"}
		funcDefaultRow := []driver.Value{"public", "func_name", "f", "SELECT $1+$2", "", "integer, integer", "integer, integer", "integer",
			"v", "f", "f", "", "100", "0", "", "sql", "comment", "owner"}

		It("returns a slice of function definitions", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(funcDefaultRow...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.GetFunctionDefinitions(connection)
			Expect(result[0].SchemaName).To(Equal("public"))
			Expect(result[0].FunctionName).To(Equal("func_name"))
			Expect(result[0].ReturnsSet).To(BeFalse())
			Expect(result[0].FunctionBody).To(Equal("SELECT $1+$2"))
			Expect(result[0].BinaryPath).To(Equal(""))
			Expect(result[0].Arguments).To(Equal("integer, integer"))
			Expect(result[0].IdentArgs).To(Equal("integer, integer"))
			Expect(result[0].ResultType).To(Equal("integer"))
			Expect(result[0].Volatility).To(Equal("v"))
			Expect(result[0].IsStrict).To(BeFalse())
			Expect(result[0].IsSecurityDefiner).To(BeFalse())
			Expect(result[0].Config).To(Equal(""))
			Expect(result[0].Cost).To(Equal(float32(100)))
			Expect(result[0].NumRows).To(Equal(float32(0)))
			Expect(result[0].SqlUsage).To(Equal(""))
			Expect(result[0].Language).To(Equal("sql"))
			Expect(result[0].Comment).To(Equal("comment"))
			Expect(result[0].Owner).To(Equal("owner"))
		})
	})
	Describe("GetAggregateDefinitions", func() {
		header := []string{"nspname", "proname", "arguments", "identargs", "aggtransfn", "aggprelimfn", "aggfinalfn", "aggsortop",
			"transitiondatatype", "initialvalue", "aggordered", "comment", "owner"}
		aggDefaultRow := []driver.Value{"public", "agg_name", "integer, integer", "integer, integer", 1, 2, 3, 4,
			"integer", "0", "f", "comment", "owner"}

		It("returns a slice of aggregate definitions", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(aggDefaultRow...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.GetAggregateDefinitions(connection)
			Expect(result[0].SchemaName).To(Equal("public"))
			Expect(result[0].AggregateName).To(Equal("agg_name"))
			Expect(result[0].Arguments).To(Equal("integer, integer"))
			Expect(result[0].IdentArgs).To(Equal("integer, integer"))
			Expect(result[0].TransitionFunction).To(Equal(uint32(1)))
			Expect(result[0].PreliminaryFunction).To(Equal(uint32(2)))
			Expect(result[0].FinalFunction).To(Equal(uint32(3)))
			Expect(result[0].SortOperator).To(Equal(uint32(4)))
			Expect(result[0].TransitionDataType).To(Equal("integer"))
			Expect(result[0].InitialValue).To(Equal("0"))
			Expect(result[0].IsOrdered).To(BeFalse())
			Expect(result[0].Comment).To(Equal("comment"))
			Expect(result[0].Owner).To(Equal("owner"))
		})
	})
})

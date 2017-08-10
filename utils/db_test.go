package utils_test

import (
	"fmt"
	"os"
	"time"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var connection *utils.DBConn
var mock sqlmock.Sqlmock

var _ = Describe("utils/db tests", func() {
	BeforeEach(func() {
		utils.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
	})
	Describe("NewDBConn", func() {
		It("gets the DBName from the dbname flag if it is set", func() {
			connection = utils.NewDBConn("testdb")
			Expect(connection.DBName).To(Equal("testdb"))
		})
		It("gets the DBName from PGDATABASE if the dbname flag is not set", func() {
			oldPgDatabase := os.Getenv("PGDATABASE")
			os.Setenv("PGDATABASE", "testdb")
			defer os.Setenv("PGDATABASE", oldPgDatabase)

			connection = utils.NewDBConn("")
			Expect(connection.DBName).To(Equal("testdb"))
		})
		It("fails if no database is given with either -dbname or PGDATABASE", func() {
			oldPgDatabase := os.Getenv("PGDATABASE")
			os.Setenv("PGDATABASE", "")
			defer os.Setenv("PGDATABASE", oldPgDatabase)

			defer testutils.ShouldPanicWithMessage("No database provided and PGDATABASE not set")
			connection = utils.NewDBConn("")
		})
	})
	Describe("DBConn.Connect", func() {
		It("connects successfully if the database exists", func() {
			var mockdb *sqlx.DB
			mockdb, mock = testutils.CreateMockDB()
			driver := testutils.TestDriver{DB: mockdb, User: "testrole"}
			connection = utils.NewDBConn("testdb")
			connection.Driver = driver
			Expect(connection.DBName).To(Equal("testdb"))
			connection.Connect()
		})
		It("does not connect if the database exists but the connection is refused", func() {
			var mockdb *sqlx.DB
			mockdb, mock = testutils.CreateMockDB()
			driver := testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: connection refused"), DB: mockdb, User: "testrole"}
			connection = utils.NewDBConn("testdb")
			connection.Driver = driver
			defer testutils.ShouldPanicWithMessage(`could not connect to server: Connection refused`)
			connection.Connect()
		})
		It("fails if the database does not exist", func() {
			var mockdb *sqlx.DB
			mockdb, mock = testutils.CreateMockDB()
			driver := testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: database \"testdb\" does not exist"), DB: mockdb, DBName: "testdb", User: "testrole"}
			connection = utils.NewDBConn("testdb")
			connection.Driver = driver
			Expect(connection.DBName).To(Equal("testdb"))
			defer testutils.ShouldPanicWithMessage("Database \"testdb\" does not exist, exiting")
			connection.Connect()
		})
		It("fails if the role does not exist", func() {
			var mockdb *sqlx.DB
			mockdb, mock = testutils.CreateMockDB()
			driver := testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: role \"nonexistent\" does not exist"), DB: mockdb, DBName: "testdb", User: "nonexistent"}

			oldPgUser := os.Getenv("PGUSER")
			os.Setenv("PGUSER", "nonexistent")
			defer os.Setenv("PGUSER", oldPgUser)

			connection = utils.NewDBConn("testdb")
			connection.Driver = driver
			Expect(connection.User).To(Equal("nonexistent"))
			defer testutils.ShouldPanicWithMessage("Role \"nonexistent\" does not exist, exiting")
			connection.Connect()
		})
	})
	Describe("DBConn.Exec", func() {
		It("executes an INSERT outside of a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			fakeResult := testutils.TestResult{Rows: 1}
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)

			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
		It("executes an INSERT in a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			fakeResult := testutils.TestResult{Rows: 1}
			testutils.ExpectBegin(mock)
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)
			mock.ExpectCommit()

			connection.Begin()
			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			connection.Commit()
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
	})
	Describe("DBConn.Get", func() {
		It("executes a GET outside of a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")

			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
		It("executes a GET in a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			testutils.ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)
			mock.ExpectCommit()

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			connection.Begin()
			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")
			connection.Commit()
			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
	})
	Describe("DBConn.Select", func() {
		It("executes a SELECT outside of a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
		It("executes a SELECT in a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			testutils.ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)
			mock.ExpectCommit()

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			connection.Begin()
			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")
			connection.Commit()

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
	})
	Describe("DBConn.Begin", func() {
		It("successfully executes a BEGIN outside a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			testutils.ExpectBegin(mock)
			connection.Begin()
			Expect(connection.Tx).To(Not(BeNil()))
		})
		It("panics if it executes a BEGIN in a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			testutils.ExpectBegin(mock)
			connection.Begin()
			defer testutils.ShouldPanicWithMessage("Cannot begin transaction; there is already a transaction in progress")
			connection.Begin()
		})
	})
	Describe("DBConn.Commit", func() {
		It("successfully executes a COMMIT in a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			testutils.ExpectBegin(mock)
			mock.ExpectCommit()
			connection.Begin()
			connection.Commit()
			Expect(connection.Tx).To(BeNil())
		})
		It("panics if it executes a COMMIT outside a transaction", func() {
			connection, mock = testutils.CreateAndConnectMockDB()
			defer testutils.ShouldPanicWithMessage("Cannot commit transaction; there is no transaction in progress")
			connection.Commit()
		})
	})
})

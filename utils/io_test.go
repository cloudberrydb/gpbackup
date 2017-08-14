package utils_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"os"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("utils/io tests", func() {
	Describe("QuoteIdent", func() {
		It("does not quote a string that contains no special characters", func() {
			name := `_tablename1`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`_tablename1`))
		})
		It("replaces double quotes with pairs of double quotes", func() {
			name := `table"name`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`"table""name"`))
		})
		It("replaces backslashes with pairs of backslashes", func() {
			name := `table\name`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`"table\\name"`))
		})
		It("properly escapes capital letters", func() {
			names := []string{"Relationname", "TABLENAME", "TaBlEnAmE"}
			expected := []string{`"Relationname"`, `"TABLENAME"`, `"TaBlEnAmE"`}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes shell-significant special characters", func() {
			special := `.,!$/` + "`"
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes whitespace", func() {
			names := []string{"table name", "table\tname", "table\nname"}
			expected := []string{`"table name"`, "\"table\tname\"", "\"table\nname\""}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes all other punctuation", func() {
			special := `'~@#$%^&*()-+[]{}><|;:?`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes Unicode characters", func() {
			special := `Ää表`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
	})
	Describe("CreateDirectoryOnMaster", func() {
		It("does nothing if the directory exists", func() {
			fakeInfo, _ := os.Stat("/tmp/log_dir")
			utils.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, nil }
			defer func() { utils.System.Stat = os.Stat }()
			utils.CreateDirectoryOnMaster("dirname")
		})
		It("panics if the directory does not exist", func() {
			utils.System.Stat = func(name string) (os.FileInfo, error) { return nil, errors.New("No such file or directory") }
			defer func() { utils.System.Stat = os.Stat }()
			defer testutils.ShouldPanicWithMessage("Cannot stat directory dirname: No such file or directory")
			utils.CreateDirectoryOnMaster("dirname")
		})
	})

	Describe("MustOpenFileForWriting", func() {
		It("creates or opens the file for writing", func() {
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return os.Stdout, nil }
			defer func() { utils.System.OpenFile = os.OpenFile }()
			fileHandle := utils.MustOpenFileForWriting("filename")
			Expect(fileHandle).To(Equal(os.Stdout))
		})
		It("panics on error", func() {
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return nil, errors.New("Permission denied")
			}
			defer func() { utils.System.OpenFile = os.OpenFile }()
			defer testutils.ShouldPanicWithMessage("Unable to create or open file for writing: Permission denied")
			utils.MustOpenFileForWriting("filename")
		})
	})
	Describe("MustOpenFileForReading", func() {
		It("creates or opens the file for reading", func() {
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return os.Stdin, nil }
			defer func() { utils.System.OpenFile = os.OpenFile }()
			fileHandle := utils.MustOpenFileForReading("filename")
			Expect(fileHandle).To(Equal(os.Stdin))
		})
		It("panics on error", func() {
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return nil, errors.New("Permission denied")
			}
			defer func() { utils.System.OpenFile = os.OpenFile }()
			defer testutils.ShouldPanicWithMessage("Unable to open file for reading: Permission denied")
			utils.MustOpenFileForReading("filename")
		})
	})
	Describe("GetSegmentConfiguration", func() {
		header := []string{"contentid", "hostname", "datadir"}
		localSegOne := []driver.Value{"0", "localhost", "/data/gpseg0"}
		localSegTwo := []driver.Value{"1", "localhost", "/data/gpseg1"}
		remoteSegOne := []driver.Value{"2", "remotehost", "/data/gpseg2"}

		It("returns a configuration for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
	})
	Describe("MustPrintf", func() {
		It("writes to a writable file", func() {
			utils.MustPrintf(buffer, "%s", "text")
			Expect(string(buffer.Contents())).To(Equal("text"))
		})
		It("panics on error", func() {
			defer testutils.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintf(os.Stdin, "text")
		})
	})
	Describe("MustPrintln", func() {
		It("writes to a writable file", func() {
			utils.MustPrintln(buffer, "text")
			Expect(string(buffer.Contents())).To(Equal("text\n"))
		})
		It("panics on error", func() {
			defer testutils.ShouldPanicWithMessage("write /dev/stdin:")
			utils.MustPrintln(os.Stdin, "text")
		})
	})
})

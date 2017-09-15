package utils_test

import (
	"os"
	"strings"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

var _ = Describe("utils/report tests", func() {
	Describe("ParseErrorMessage", func() {
		It("Parses a CRITICAL error message and returns error code 1", func() {
			var err interface{}
			err = "testProgram:testUser:testHost:000000-[CRITICAL]:-Error Message"
			errMsg, exitCode := utils.ParseErrorMessage(err)
			Expect(errMsg).To(Equal("Error Message"))
			Expect(exitCode).To(Equal(1))
		})
		It("Returns error code 0 for an empty error message", func() {
			errMsg, exitCode := utils.ParseErrorMessage(nil)
			Expect(errMsg).To(Equal(""))
			Expect(exitCode).To(Equal(0))
		})
	})
	Describe("WriteReportFile", func() {
		timestamp := "20170101010101"
		backupReport := &utils.Report{
			BackupType:      "Unfiltered Full Backup",
			BackupVersion:   "0.1.0",
			DatabaseName:    "testdb",
			DatabaseSize:    "42 MB",
			DatabaseVersion: "5.0.0 build test",
		}
		objectCounts := map[string]int{"tables": 42, "sequences": 1, "types": 1000}

		It("writes a report for a successful backup", func() {
			utils.WriteReportFile(buffer, timestamp, backupReport, objectCounts, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Database Name: testdb
Command Line: .*
Backup Type: Unfiltered Full Backup
Backup Status: Success

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                    1
tables                       42
types                        1000`))
		})
		It("writes a report for a failed backup", func() {
			utils.WriteReportFile(buffer, timestamp, backupReport, objectCounts, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Database Name: testdb
Command Line: .*
Backup Type: Unfiltered Full Backup
Backup Status: Failure
Backup Error: Cannot access /tmp/backups: Permission denied

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                    1
tables                       42
types                        1000`))
		})
	})
	Describe("ReadReportFile", func() {
		It("can read a report file for a successful backup", func() {
			reportFileContents := `Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5.0.0 build test
gpbackup Version: 0.1.0

Database Name: testdb
Command Line: gpbackup --dbname testdb
Backup Type: Unfiltered Full Backup
Backup Status: Success

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                   1
tables                      42
types                       1000
`
			reportReader := strings.NewReader(reportFileContents)
			backupReport := utils.ReadReportFile(reportReader)
			expectedReport := utils.Report{DatabaseName: "testdb", DatabaseVersion: "5.0.0 build test", BackupVersion: "0.1.0", BackupType: "Unfiltered Full Backup"}
			testutils.ExpectStructsToMatch(&expectedReport, &backupReport)
		})
		It("can read a report file for a failed backup", func() {
			reportFileContents := `Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5.0.0 build test
gpbackup Version: 0.1.0

Database Name: testdb
Command Line: gpbackup --dbname testdb
Backup Type: Unfiltered Full Backup
Backup Status: Failure
Backup Error: Cannot access /tmp/backups: Permission denied

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                   1
tables                      42
types                       1000
`
			reportReader := strings.NewReader(reportFileContents)
			backupReport := utils.ReadReportFile(reportReader)
			expectedReport := utils.Report{DatabaseName: "testdb", DatabaseVersion: "5.0.0 build test", BackupVersion: "0.1.0", BackupType: "Unfiltered Full Backup"}
			testutils.ExpectStructsToMatch(&expectedReport, &backupReport)
		})
	})
	Describe("SetBackupTypeFromFlags", func() {
		var backupReport *utils.Report
		BeforeEach(func() {
			backupReport = &utils.Report{}
		})
		DescribeTable("Backup type classification", func(dataOnly bool, ddlOnly bool, noCompression bool, schemaInclude utils.ArrayFlags, expectedType string) {
			backupReport.SetBackupTypeFromFlags(dataOnly, ddlOnly, noCompression, schemaInclude)
			Expect(backupReport.BackupType).To(Equal(expectedType))
		},
			Entry("classifies a default backup",
				false, false, false, utils.ArrayFlags{}, "Unfiltered Compressed Full Backup"),
			Entry("classifies a metadata-only backup",
				false, true, false, utils.ArrayFlags{}, "Unfiltered Compressed Full Metadata-Only Backup"),
			Entry("classifies a data-only backup",
				true, false, false, utils.ArrayFlags{}, "Unfiltered Compressed Full Data-Only Backup"),
			Entry("classifies an uncompressed backup",
				false, false, true, utils.ArrayFlags{}, "Unfiltered Uncompressed Full Backup"),
			Entry("classifies an uncompressed metadata-only backup",
				false, true, true, utils.ArrayFlags{}, "Unfiltered Uncompressed Full Metadata-Only Backup"),
			Entry("classifies an uncompressed data-only backup",
				true, false, true, utils.ArrayFlags{}, "Unfiltered Uncompressed Full Data-Only Backup"),
			Entry("classifies a schema-filtered backup",
				false, false, false, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Compressed Full Backup"),
			Entry("classifies a schema-filtered metadata-only backup",
				false, true, false, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Compressed Full Metadata-Only Backup"),
			Entry("classifies a schema-filtered data-only backup",
				true, false, false, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Compressed Full Data-Only Backup"),
			Entry("classifies an uncompressed schema-filtered backup",
				false, false, true, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Uncompressed Full Backup"),
			Entry("classifies an uncompressed schema-filtered metadata-only backup",
				false, true, true, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Uncompressed Full Metadata-Only Backup"),
			Entry("classifies an uncompressed schema-filtered data-only backup",
				true, false, true, utils.ArrayFlags{"someSchema"}, "Schema-Filtered Uncompressed Full Data-Only Backup"),
		)
	})
	Describe("SetBackupTypeFromString", func() {
		var backupReport *utils.Report
		BeforeEach(func() {
			backupReport = &utils.Report{}
		})
		DescribeTable("Backup type classification", func(dataOnly bool, ddlOnly bool, noCompression bool, schemaFiltered bool, inputType string) {
			backupReport.BackupType = inputType
			backupReport.SetBackupTypeFromString()
			Expect(backupReport.DataOnly).To(Equal(dataOnly))
			Expect(backupReport.MetadataOnly).To(Equal(ddlOnly))
			Expect(backupReport.Compressed).To(Equal(!noCompression))
			Expect(backupReport.SchemaFiltered).To(Equal(schemaFiltered))
		},
			Entry("can set the type for a default backup",
				false, false, false, false, "Unfiltered Compressed Full Backup"),
			Entry("can set the type for a metadata-only backup",
				false, true, false, false, "Unfiltered Compressed Full Metadata-Only Backup"),
			Entry("can set the type for a data-only backup",
				true, false, false, false, "Unfiltered Compressed Full Data-Only Backup"),
			Entry("can set the type for an uncompressed backup",
				false, false, true, false, "Unfiltered Uncompressed Full Backup"),
			Entry("can set the type for an uncompressed metadata-only backup",
				false, true, true, false, "Unfiltered Uncompressed Full Metadata-Only Backup"),
			Entry("can set the type for an uncompressed data-only backup",
				true, false, true, false, "Unfiltered Uncompressed Full Data-Only Backup"),
			Entry("can set the type for a schema-filtered backup",
				false, false, false, true, "Schema-Filtered Compressed Full Backup"),
			Entry("can set the type for a schema-filtered metadata-only backup",
				false, true, false, true, "Schema-Filtered Compressed Full Metadata-Only Backup"),
			Entry("can set the type for a schema-filtered data-only backup",
				true, false, false, true, "Schema-Filtered Compressed Full Data-Only Backup"),
			Entry("can set the type for an uncompressed schema-filtered backup",
				false, false, true, true, "Schema-Filtered Uncompressed Full Backup"),
			Entry("can set the type for an uncompressed schema-filtered metadata-only backup",
				false, true, true, true, "Schema-Filtered Uncompressed Full Metadata-Only Backup"),
			Entry("can set the type for an uncompressed schema-filtered data-only backup",
				true, false, true, true, "Schema-Filtered Uncompressed Full Data-Only Backup"),
		)
		DescribeTable("Classification errors", func(inputType string, errorMessage string) {
			backupReport.BackupType = inputType
			defer testutils.ShouldPanicWithMessage(errorMessage)
			backupReport.SetBackupTypeFromString()
		},
			Entry("can detect an invalid format due to extra tokens",
				"Unfiltered Compressed Full Backup Extra-Token", `Invalid backup type string format: "Unfiltered Compressed Full Backup Extra-Token"`),
			Entry("can detect an invalid format due to missing tokens",
				"Unfiltered Full Backup", `Invalid backup type string format: "Unfiltered Full Backup"`),
			Entry("can detect an invalid filter string",
				"Table-Filtered Compressed Full Backup", `Invalid backup filter string: "Table-Filtered"`),
			Entry("can detect an invalid compression string",
				"Unfiltered Gzip-Compressed Full Backup", `Invalid backup compression string: "Gzip-Compressed"`),
			Entry("can detect an invalid section string",
				"Unfiltered Compressed Full Postdata-Only Backup", `Invalid backup section string: "Postdata-Only"`),
		)
	})
	Describe("Email-related functions", func() {
		reportFileContents := []byte(`Greenplum Database Backup Report

Timestamp Key: 20170101010101`)
		contactsFileContents := []byte(`contact1@example.com
contact2@example.org`)
		contactsList := "contact1@example.com contact2@example.org"

		var (
			testExecutor *testutils.TestExecutor
			testCluster  utils.Cluster
			w            *os.File
			r            *os.File
		)
		BeforeEach(func() {
			r, w, _ = os.Pipe()
			testCluster = testutils.SetDefaultSegmentConfiguration()
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return r, nil }
			utils.System.Hostname = func() (string, error) { return "localhost", nil }
			utils.System.Getenv = func(key string) string {
				if key == "HOME" {
					return "home"
				} else {
					return "gphome"
				}
			}
			testExecutor = &testutils.TestExecutor{}
			testCluster.Timestamp = "20170101010101"
			testCluster.Executor = testExecutor
		})
		AfterEach(func() {
			utils.InitializeSystemFunctions()
		})
		Context("ConstructEmailMessage", func() {
			It("adds HTML formatting to the contents of the report file", func() {
				w.Write(reportFileContents)
				w.Close()

				message := utils.ConstructEmailMessage(testCluster, contactsList)
				expectedMessage := `To: contact1@example.com contact2@example.org
Subject: gpbackup 20170101010101 on localhost completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
Greenplum Database Backup Report

Timestamp Key: 20170101010101
</pre>
</body>
</html>`
				Expect(message).To(Equal(expectedMessage))
			})
		})
		Context("EmailReport", func() {
			var (
				expectedHomeCmd   = "test -f home/mail_contacts"
				expectedGpHomeCmd = "test -f gphome/bin/mail_contacts"
				expectedMessage   = `echo "To: contact1@example.com contact2@example.org
Subject: gpbackup 20170101010101 on localhost completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">

</pre>
</body>
</html>" | sendmail -t`
			)
			It("sends no email and raises a warning if no mail_contacts file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd}))
				Expect(stdout).To(gbytes.Say("Found neither gphome/bin/mail_contacts nor home/mail_contacts"))
			})
			It("sends an email to contacts in $HOME/mail_contacts if only that file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.ErrorOnExecNum = 2 // Shouldn't hit this case, as it shouldn't be executed a second time
				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com contact2@example.org"))
			})
			It("sends an email to contacts in $GPHOME/bin/mail_contacts if only that file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.ErrorOnExecNum = 1
				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster)
				Expect(testExecutor.NumExecutions).To(Equal(3))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com contact2@example.org"))
			})
			It("sends an email to contacts in $HOME/mail_contacts if a file exists in both $HOME and $GPHOME/bin", func() {
				w.Write(contactsFileContents)
				w.Close()

				utils.EmailReport(testCluster)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com contact2@example.org"))
			})
		})
	})
})

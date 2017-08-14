package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("utils/report tests", func() {
	Describe("ParseErrorMessage", func() {
		It("Parses a CRITICAL error message and returns error code 1", func() {
			var err interface{}
			err = "testProgram:testUser:testHost:000000-[CRITICAL]:-Error Message"
			errMsg, exitCode := backup.ParseErrorMessage(err)
			Expect(errMsg).To(Equal("Error Message"))
			Expect(exitCode).To(Equal(1))
		})
		It("Returns error code 0 for an empty error message", func() {
			errMsg, exitCode := backup.ParseErrorMessage(nil)
			Expect(errMsg).To(Equal(""))
			Expect(exitCode).To(Equal(0))
		})
	})
	Describe("WriteReportFile", func() {
		backupReport := backup.Report{BackupVersion: "0.1.0", DatabaseVersion: "5.0.0 build test", BackupType: "Unfiltered Full Backup"}
		objectCounts := map[string]int{"tables": 42, "sequences": 1, "types": 1000}

		It("writes a report for a successful backup", func() {
			backup.WriteReportFile(connection, buffer, backupReport, objectCounts, "42 MB", "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Command Line: .*
Backup Type: Unfiltered Full Backup
Backup Status: Success

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                	1
tables                   	42
types                    	1000`))
		})
		It("writes a report for a failed backup", func() {
			backup.WriteReportFile(connection, buffer, backupReport, objectCounts, "42 MB", "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Command Line: .*
Backup Type: Unfiltered Full Backup
Backup Status: Failure
Backup Error: Cannot access /tmp/backups: Permission denied

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                	1
tables                   	42
types                    	1000`))
		})
	})
})

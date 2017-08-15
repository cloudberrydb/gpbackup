package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/blang/semver"
	"github.com/pkg/errors"
)

/*
 * This struct holds information that will be printed to the report file
 * after a backup that we will want to read in for a restore.
 */
type Report struct {
	BackupType      string
	BackupVersion   string
	DatabaseName    string
	DatabaseSize    string
	DatabaseVersion string
}

func ParseErrorMessage(err interface{}) (string, int) {
	if err == nil {
		return "", 0
	}
	errStr := err.(string)
	errLevelStr := "[CRITICAL]:-"
	headerIndex := strings.Index(errStr, errLevelStr)
	errMsg := errStr[headerIndex+len(errLevelStr):]
	exitCode := 1 // TODO: Define different error codes for different kinds of errors
	return errMsg, exitCode
}

func WriteReportFile(connection *DBConn, reportFile io.Writer, timestamp string, report Report, objectCounts map[string]int, errMsg string) {
	reportFileTemplate := `Greenplum Database Backup Report

Timestamp Key: %s
GPDB Version: %s
gpbackup Version: %s

Database Name: %s
Command Line: %s
Backup Type: %s
Backup Status: %s
%s
Database Size: %s`

	gpbackupCommandLine := strings.Join(os.Args, " ")
	backupStatus := "Success"
	if errMsg != "" {
		backupStatus = "Failure"
		errMsg = fmt.Sprintf("Backup Error: %s\n", errMsg)
	}
	MustPrintf(reportFile, reportFileTemplate, timestamp, report.DatabaseVersion, report.BackupVersion, report.DatabaseName,
		gpbackupCommandLine, report.BackupType, backupStatus, errMsg, report.DatabaseSize)

	objectStr := "\nCount of Database Objects in Backup:\n"
	objectSlice := make([]string, 0)
	for k := range objectCounts {
		objectSlice = append(objectSlice, k)
	}
	sort.Strings(objectSlice)
	for _, object := range objectSlice {
		objectStr += fmt.Sprintf("%-25s\t%d\n", object, objectCounts[object])

	}
	MustPrintf(reportFile, objectStr)
}

func ReadReportFile(reportFile io.Reader) Report {
	scanner := bufio.NewScanner(reportFile)
	backupReport := Report{}
	for scanner.Scan() {
		tokens := strings.SplitN(scanner.Text(), ": ", 2)
		if tokens[0] == "GPDB Version" {
			backupReport.DatabaseVersion = tokens[1]
		}
		if tokens[0] == "gpbackup Version" {
			backupReport.BackupVersion = tokens[1]
		}
		if tokens[0] == "Database Name" {
			backupReport.DatabaseName = tokens[1]
		}
		if tokens[0] == "Backup Type" {
			backupReport.BackupType = tokens[1]
			break // We don't care about the report file contents after this line
		}
	}
	return backupReport
}

/*
 * This function will not error out if the user has gprestore X.Y.Z
 * and gpbackup X.Y.Z+dev, when technically the uncommitted code changes
 * in the +dev version of gpbackup may have incompatibilities with the
 * committed version of gprestore.
 *
 * We assume this condition will never arise in practice, as gpbackup and
 * gprestore will be built with identical versions during development, and
 * users will never use a +dev version in production.
 */
func EnsureBackupVersionCompatibility(backupVersion string, restoreVersion string) {
	backupSemVer, err := semver.Make(backupVersion)
	CheckError(err)
	restoreSemVer, err := semver.Make(restoreVersion)
	CheckError(err)
	if backupSemVer.GT(restoreSemVer) {
		logger.Fatal(errors.Errorf("gprestore %s cannot restore a backup taken with gpbackup %s; please use gprestore %s or later.",
			restoreVersion, backupVersion, backupVersion), "")
	}
}

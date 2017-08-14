package backup

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This struct holds information that will be printed to the report file
 * after a backup that we will want to read in for a restore.
 */
type Report struct {
	DatabaseVersion string
	BackupVersion   string
	BackupType      string
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

func WriteReportFile(connection *utils.DBConn, reportFile io.Writer, report Report, objectCounts map[string]int, dbsize string, errMsg string) {
	reportFileTemplate := `Greenplum Database Backup Report

Timestamp Key: %s
GPDB Version: %s
gpbackup Version: %s

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
	utils.MustPrintf(reportFile, reportFileTemplate, globalCluster.Timestamp, report.DatabaseVersion, report.BackupVersion, gpbackupCommandLine, report.BackupType, backupStatus, errMsg, dbsize)

	objectStr := "\nCount of Database Objects in Backup:\n"
	objectSlice := make([]string, 0)
	for k := range objectCounts {
		objectSlice = append(objectSlice, k)
	}
	sort.Strings(objectSlice)
	for _, object := range objectSlice {
		objectStr += fmt.Sprintf("%-25s\t%d\n", object, objectCounts[object])

	}
	utils.MustPrintf(reportFile, objectStr)
}

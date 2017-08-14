package backup

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

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

func WriteReportFile(connection *utils.DBConn, reportFile io.Writer, objectCounts map[string]int, errMsg string) {
	reportFileTemplate := `Greenplum Database Backup Report

Timestamp Key: %s
GPDB Version: %s
gpbackup Version: %s

Command Line: %s
Backup Type: %s
Backup Status: %s
%s`

	gpbackupCommandLine := strings.Join(os.Args, " ")
	backupType := "Full Unfiltered"
	backupStatus := "Success"
	gpdbVersion := connection.GPDBVersion
	if errMsg != "" {
		backupStatus = "Failure"
		errMsg = "\nBackup Error: " + errMsg
	}
	utils.MustPrintf(reportFile, reportFileTemplate, globalCluster.Timestamp, gpdbVersion, version, gpbackupCommandLine, backupType, backupStatus, errMsg)

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

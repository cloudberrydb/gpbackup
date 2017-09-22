package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
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
	Compressed      bool
	DataOnly        bool
	SchemaFiltered  bool
	MetadataOnly    bool
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

func (report *Report) SetBackupTypeFromFlags(dataOnly bool, ddlOnly bool, noCompression bool, schemaInclude ArrayFlags) {
	filterStr := "Unfiltered"
	if len(schemaInclude) > 0 {
		report.SchemaFiltered = true
		filterStr = "Schema-Filtered"
	}
	compressStr := "Compressed"
	if noCompression {
		compressStr = "Uncompressed"
	} else {
		report.Compressed = true
	}
	sectionStr := ""
	if dataOnly {
		report.DataOnly = true
		sectionStr = " Data-Only"
	}
	if ddlOnly {
		report.MetadataOnly = true
		sectionStr = " Metadata-Only"
	}
	report.BackupType = fmt.Sprintf("%s %s Full%s Backup", filterStr, compressStr, sectionStr)
}

func WriteReportFile(reportFile io.Writer, timestamp string, report *Report, objectCounts map[string]int, errMsg string) {
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
		objectStr += fmt.Sprintf("%-29s%d\n", object, objectCounts[object])

	}
	MustPrintf(reportFile, objectStr)
}

func (report *Report) SetBackupTypeFromString() {
	typeRegexp := regexp.MustCompile(`^([^ ]+) ([^ ]+) Full( [^ ]+)? Backup$`)
	tokens := typeRegexp.FindStringSubmatch(report.BackupType)
	if tokens == nil || len(tokens) == 0 {
		logger.Fatal(errors.Errorf(`Invalid backup type string format: "%s" (Valid format is "[Unfiltered|Schema-Filtered] [Compressed|Uncompressed] Full[| Metadata-Only| Data-Only] Backup")`, report.BackupType), "")
	}
	if tokens[1] == "Schema-Filtered" {
		report.SchemaFiltered = true
	} else if tokens[1] != "Unfiltered" {
		logger.Fatal(errors.Errorf(`Invalid backup filter string: "%s"`, tokens[1]), "Could not determine whether filter was used in backup")
	}
	if tokens[2] == "Compressed" {
		report.Compressed = true
	} else if tokens[2] != "Uncompressed" {
		logger.Fatal(errors.Errorf(`Invalid backup compression string: "%s"`, tokens[2]), "Could not determine whether compression was used in backup")
	}
	if tokens[3] == " Data-Only" {
		report.DataOnly = true
	} else if tokens[3] == " Metadata-Only" {
		report.MetadataOnly = true
	} else if tokens[3] != "" {
		logger.Fatal(errors.Errorf(`Invalid backup section string: "%s"`, tokens[3][1:]), "Could not determine whether backup included metadata, data, or both")
	}
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

func EnsureDatabaseVersionCompatibility(backupGPDBVersion string, restoreGPDBVersion GPDBVersion) {
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(backupGPDBVersion)[0]
	backupGPDBSemVer, err := semver.Make(threeDigitVersion)
	CheckError(err)
	if backupGPDBSemVer.Major > restoreGPDBVersion.SemVer.Major {
		logger.Fatal(errors.Errorf("Cannot restore from GPDB version %s to %s due to catalog incompatibilities.", backupGPDBVersion, restoreGPDBVersion.VersionString), "")
	}
}

func ConstructEmailMessage(cluster Cluster, contactList string) string {
	hostname, _ := System.Hostname()
	emailHeader := fmt.Sprintf(`To: %s
Subject: gpbackup %s on %s completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
`, contactList, cluster.Timestamp, hostname)
	emailFooter := `
</pre>
</body>
</html>`
	fileContents := strings.Join(ReadLinesFromFile(cluster.GetReportFilePath()), "\n")
	return emailHeader + fileContents + emailFooter
}

func EmailReport(cluster Cluster) {
	contactsFilename := "mail_contacts"
	gphomeFile := fmt.Sprintf("%s/bin/%s", System.Getenv("GPHOME"), contactsFilename)
	homeFile := fmt.Sprintf("%s/%s", System.Getenv("HOME"), contactsFilename)
	homeErr := cluster.ExecuteLocalCommand(fmt.Sprintf("test -f %s", homeFile))
	if homeErr != nil {
		gphomeErr := cluster.ExecuteLocalCommand(fmt.Sprintf("test -f %s", gphomeFile))
		if gphomeErr != nil {
			logger.Warn("Found neither %s nor %s", gphomeFile, homeFile)
			logger.Warn("Unable to send backup email notification")
			return
		}
		contactsFilename = gphomeFile
	} else {
		contactsFilename = homeFile
	}
	contactList := strings.Join(ReadLinesFromFile(contactsFilename), " ")
	message := ConstructEmailMessage(cluster, contactList)
	logger.Verbose("Sending email report to the following addresses: %s", contactList)
	sendErr := cluster.ExecuteLocalCommand(fmt.Sprintf(`echo "%s" | sendmail -t`, message))
	if sendErr != nil {
		logger.Warn("Unable to send email report: %s", sendErr.Error())
	}
}

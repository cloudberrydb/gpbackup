package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const DefaultSegmentDir = "<SEG_DATA_DIR>"

var (
	BaseDumpDir = DefaultSegmentDir

	contentList []int
	segDirMap   map[int]string
	segHostMap  map[int]string

	/* To be used in a Postgres query without being quoted, an identifier (schema or
	 * table) must begin with a lowercase letter or underscore, and may contain only
	 * lowercast letters, digits, and underscores.
	 */
	UnquotedIdentifier = regexp.MustCompile(`^([a-z_][a-z0-9_]*)$`)
	QuotedIdentifier   = regexp.MustCompile(`^"(.+)"$`)

	QuotedOrUnquotedString = regexp.MustCompile(`^(?:\"(.*)\"|(.*))\.(?:\"(.*)\"|(.*))$`)

	// Swap between double quotes and paired double quotes, and between literal whitespace characters and escape sequences
	ReplacerEscape   = strings.NewReplacer(`"`, `""`, `\`, `\\`)
	ReplacerUnescape = strings.NewReplacer(`""`, `"`, `\\`, `\`)
)

// This function quotes an unquoted identifier like quote_ident() in Postgres.
func QuoteIdent(ident string) string {
	if !UnquotedIdentifier.MatchString(ident) {
		ident = ReplacerEscape.Replace(ident)
		ident = fmt.Sprintf(`"%s"`, ident)
	}
	return ident
}

/*
 * Generic file/directory manipulation functions
 */

func CheckDirectoryExists(dirname string) (bool, error) {
	info, err := System.Stat(dirname)
	if err != nil {
		if System.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if !(info.IsDir()) {
		return false, errors.Errorf("%s is a file, not a directory", dirname)
	}
	return true, nil
}

func DirectoryMustExist(dirname string) {
	info, err := System.Stat(dirname)
	if err != nil {
		if System.IsNotExist(err) {
			err = System.MkdirAll(dirname, 0755)
			if err != nil {
				logger.Fatal(err, "Cannot create directory %s", dirname)
			}
		} else {
			logger.Fatal(err, "Cannot stat directory %s", dirname)
		}
	} else if !(info.IsDir()) {
		logger.Fatal(errors.Errorf("%s is a file, not a directory", dirname), "")
	}
}

func MustOpenFile(filename string) io.Writer {
	fileHandle, err := System.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logger.Fatal(err, "Unable to create or open file")
	}
	return fileHandle
}

func GetUserAndHostInfo() (string, string, string) {
	currentUser, _ := System.CurrentUser()
	userName := currentUser.Username
	userDir := currentUser.HomeDir
	hostname, _ := System.Hostname()
	return userName, userDir, hostname
}

func ExecuteSQLFile(dbconn *DBConn, filename string) {
	connStr := []string{
		"-U", dbconn.User,
		"-d", fmt.Sprintf("%s", QuoteIdent(dbconn.DBName)),
		"-h", dbconn.Host,
		"-p", fmt.Sprintf("%d", dbconn.Port),
		"-f", fmt.Sprintf("%s", filename),
		"-v", "ON_ERROR_STOP=1",
		"-q",
	}
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		/*
		 * Not using logger.Fatal, as this is a SQL error rather than a code error,
		 * so we don't want a stack trace.
		 */
		logger.Error("Execution of SQL file encountered an error: %s", out)
		Abort()
	}
}

func MustPrintf(file io.Writer, s string, v ...interface{}) {
	_, err := fmt.Fprintf(file, s, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
}

func MustPrintln(file io.Writer, v ...interface{}) {
	_, err := fmt.Fprintln(file, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
}

/*
 * Backup-specific file/directory manipulation functions
 */

// TODO: Handle multi-node clusters
func CreateDumpDirs() {
	for segID, dumpPath := range segDirMap {
		logger.Verbose("Creating directory %s", dumpPath)
		err := System.MkdirAll(dumpPath, 0700)
		if err != nil {
			logger.Fatal(err, "Cannot create directory %s on host %s", dumpPath, segHostMap[segID])
		}
		CheckError(err)
	}
}

// TODO: Handle multi-node clusters
func AssertDumpDirsExist() {
	for _, dumpPath := range segDirMap {
		exists, err := CheckDirectoryExists(dumpPath)
		if err != nil {
			logger.Fatal(err, "Error statting dump directory %s", dumpPath)
		}
		if !exists {
			logger.Fatal(errors.Errorf("Dump directory %s does not exist", dumpPath), "")
		}
	}
}

/*
 * Functions for working with the segment configuration
 */

type QuerySegConfig struct {
	Content  int
	Hostname string
	DataDir  string
}

func GetSegmentConfiguration(connection *DBConn) []QuerySegConfig {
	query := `
SELECT
	s.content,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`

	results := make([]QuerySegConfig, 0)
	err := connection.Select(&results, query)
	CheckError(err)
	return results
}

func SetupSegmentConfiguration(segConfigs []QuerySegConfig) {
	contentList = make([]int, 0)
	segDirMap = make(map[int]string, 0)
	segHostMap = make(map[int]string, 0)
	for _, seg := range segConfigs {
		dumpPath := strings.Replace(GetGenericSegDir(), DefaultSegmentDir, seg.DataDir, -1)
		contentList = append(contentList, seg.Content)
		segDirMap[seg.Content] = dumpPath
		segHostMap[seg.Content] = seg.Hostname
	}
}

func GetContentList() []int {
	return contentList
}

func GetHostForContent(content int) string {
	return segHostMap[content]
}

func GetDirForContent(content int) string {
	return segDirMap[content]
}

/*
 * Returns a segment directory with the BaseDumpDir intact (for use with <SEG_DUMP_DIR>
 * in COPY ... ON SEGMENT), instead of a directory corresponding to a particular segment.
 */
func GetGenericSegDir() string {
	return fmt.Sprintf("%s/backups/%s/%s", BaseDumpDir, DumpTimestamp[0:8], DumpTimestamp)
}

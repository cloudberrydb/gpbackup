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

var (
	/* To be used in a Postgres query without being quoted, an identifier (schema or
	 * table) must begin with a lowercase letter or underscore, and may contain only
	 * lowercast letters, digits, and underscores.
	 */
	UnquotedIdentifier = regexp.MustCompile(`^([a-z_][a-z0-9_]*)$`)
	QuotedIdentifier   = regexp.MustCompile(`^"(.+)"$`)

	QuotedOrUnquotedString = regexp.MustCompile(`^(?:\"(.*)\"|([^.]*))\.(?:\"(.*)\"|([^.]*))$`)

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

func MustOpenFileForWriting(filename string) io.WriteCloser {
	fileHandle, err := System.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Fatal(err, "Unable to create or open file for writing")
	}
	return fileHandle
}

func MustOpenFileForReading(filename string) io.ReadCloser {
	fileHandle, err := System.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		logger.Fatal(err, "Unable to open file for reading")
	}
	return fileHandle
}

func MustOpenFileForReaderAt(filename string) io.ReaderAt {
	fileHandle, err := System.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		logger.Fatal(err, "Unable to open file for reading")
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

func MustPrintf(file io.Writer, s string, v ...interface{}) uint64 {
	bytesWritten, err := fmt.Fprintf(file, s, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
	return uint64(bytesWritten)
}

func MustPrintln(file io.Writer, v ...interface{}) uint64 {
	bytesWritten, err := fmt.Fprintln(file, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
	return uint64(bytesWritten)
}

func MustPrintBytes(file io.Writer, bytes []byte) uint64 {
	bytesWritten, err := file.Write(bytes)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
	return uint64(bytesWritten)
}

/*
 * Functions for working with the segment configuration
 */

type SegConfig struct {
	ContentID int
	Hostname  string
	DataDir   string
}

func GetSegmentConfiguration(connection *DBConn) []SegConfig {
	query := `
SELECT
	s.content as contentid,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	CheckError(err)
	return results
}

/*
 * Generic file/directory manipulation functions
 */

func CreateDirectoryOnMaster(dirname string) {
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

type FileWithByteCount struct {
	writer    io.Writer
	closer    io.WriteCloser
	ByteCount uint64
}

func NewFileWithByteCount(writer io.Writer) *FileWithByteCount {
	return &FileWithByteCount{writer, nil, 0}
}

func NewFileWithByteCountFromFile(filename string) *FileWithByteCount {
	file := MustOpenFileForWriting(filename)
	return &FileWithByteCount{file, file, 0}
}

func (file *FileWithByteCount) Close() {
	if file.closer != nil {
		file.closer.Close()
	}
}

func (file *FileWithByteCount) MustPrintln(v ...interface{}) {
	bytesWritten, err := fmt.Fprintln(file.writer, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
	file.ByteCount += uint64(bytesWritten)
}

func (file *FileWithByteCount) MustPrintf(s string, v ...interface{}) {
	bytesWritten, err := fmt.Fprintf(file.writer, s, v...)
	if err != nil {
		logger.Fatal(err, "Unable to write to file")
	}
	file.ByteCount += uint64(bytesWritten)
}

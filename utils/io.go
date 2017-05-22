package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"io"
	"strings"
)

const DefaultSegmentDir = "<SEG_DATA_DIR>"

var (
	BaseDumpDir = DefaultSegmentDir

	contentList []int
	segDirMap  map[int]string
	segHostMap map[int]string
)

/*
 * Generic file/directory manipulation functions
 */

func DirectoryMustExist(dirname string) {
	_, statErr := System.Stat(dirname)
	if statErr != nil {
		logger.Fatal("Cannot stat directory %s: %s", dirname, statErr)
	}
}

func MustOpenFile(filename string) io.Writer {
	logFileHandle, err := System.Create(filename)
	if err != nil {
		logger.Fatal("Unable to create or open file %s: %s", filename, err)
	}
	return logFileHandle
}

func GetUserAndHostInfo() (string, string, string) {
	currentUser, _ := System.CurrentUser()
	userName := currentUser.Username
	userDir := currentUser.HomeDir
	hostname, _ := System.Hostname()
	return userName, userDir, hostname
}

/*
 * Backup-specific file/directory manipulation functions
 */

// TODO: Handle multi-node clusters
func CreateDumpDirs() {
	for segId, dumpPath := range segDirMap {
		logger.Verbose("Creating directory %s", dumpPath)
		err := System.MkdirAll(dumpPath, 0700)
		if err != nil {
			logger.Fatal("Cannot create directory %s on host %s: %s", dumpPath, segHostMap[segId], err.Error())
		}
		CheckError(err)
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
	query := `SELECT
content,
hostname,
fselocation as datadir
FROM pg_catalog.gp_segment_configuration
JOIN pg_catalog.pg_filespace_entry
ON (dbid = fsedbid)
WHERE role = 'p'
ORDER BY content;`

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

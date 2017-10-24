package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	usingCompression   = true
	compressionProgram Compression
)

type Compression struct {
	Name              string
	CompressCommand   string
	DecompressCommand string
	Extension         string
}

func InitializeCompressionParameters(compress bool) {
	usingCompression = compress
	compressionProgram = Compression{Name: "gzip", CompressCommand: "gzip -c", DecompressCommand: "gzip -d", Extension: ".gz"}
}

func GetCompressionParameters() (bool, Compression) {
	return usingCompression, compressionProgram
}

func SetCompressionParameters(compress bool, compression Compression) {
	usingCompression = compress
	compressionProgram = compression
}

type Executor interface {
	ExecuteLocalCommand(commandStr string) error
	ExecuteClusterCommand(commandMap map[int][]string) map[int]error
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

type Cluster struct {
	ContentIDs             []int
	SegDirMap              map[int]string
	SegHostMap             map[int]string
	UserSpecifiedBackupDir string
	Timestamp              string
	Executor
}

type SegConfig struct {
	ContentID int
	Hostname  string
	DataDir   string
}

/*
 * Cluster functions
 */

func (cluster *Cluster) IsUserSpecifiedBackupDir() bool {
	return cluster.UserSpecifiedBackupDir != ""
}

func NewCluster(segConfigs []SegConfig, userSpecifiedBackupDir string, timestamp string) Cluster {
	cluster := Cluster{}
	cluster.SegHostMap = make(map[int]string, 0)
	cluster.SegDirMap = make(map[int]string, 0)
	cluster.UserSpecifiedBackupDir = userSpecifiedBackupDir
	cluster.Timestamp = timestamp
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.SegDirMap[seg.ContentID] = seg.DataDir
		cluster.SegHostMap[seg.ContentID] = seg.Hostname
	}
	cluster.Executor = &GPDBExecutor{}
	return cluster
}

func (cluster *Cluster) GenerateSSHCommandMap(includeMaster bool, generateCommand func(int) string) map[int][]string {
	commandMap := make(map[int][]string, len(cluster.ContentIDs))
	for _, contentID := range cluster.ContentIDs {
		if contentID == -1 && !includeMaster {
			continue
		}
		host := cluster.GetHostForContent(contentID)
		cmdStr := generateCommand(contentID)
		if contentID == -1 {
			commandMap[contentID] = []string{"bash", "-c", cmdStr}
		} else {
			commandMap[contentID] = ConstructSSHCommand(host, cmdStr)
		}
	}
	return commandMap
}

func (cluster *Cluster) GenerateSSHCommandMapForCluster(generateCommand func(int) string) map[int][]string {
	return cluster.GenerateSSHCommandMap(true, generateCommand)
}

func (cluster *Cluster) GenerateSSHCommandMapForSegments(generateCommand func(int) string) map[int][]string {
	return cluster.GenerateSSHCommandMap(false, generateCommand)
}

func (cluster *Cluster) GenerateFileVerificationCommandMap(fileCount int) map[int][]string {
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l | grep %d", cluster.GetDirForContent(contentID), fileCount)
	})
	return commandMap
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) error {
	_, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return err
}

func (executor *GPDBExecutor) ExecuteClusterCommand(commandMap map[int][]string) map[int]error {
	errMap := make(map[int]error)
	finished := make(chan int)
	contentIDs := make([]int, 0)
	for key := range commandMap {
		contentIDs = append(contentIDs, key)
	}
	errorList := make([]error, len(contentIDs))
	for i, contentID := range contentIDs {
		go func(index int, segCommand []string) {
			_, errorList[index] = exec.Command(segCommand[0], segCommand[1:]...).CombinedOutput()
			finished <- index
		}(i, commandMap[contentID])
	}
	for i := 0; i < len(contentIDs); i++ {
		index := <-finished
		hostErr := errorList[index]
		if hostErr != nil {
			errMap[contentIDs[index]] = hostErr
		}
	}
	return errMap
}

func (cluster *Cluster) VerifyBackupFileCountOnSegments(fileCount int) {
	commandMap := cluster.GenerateFileVerificationCommandMap(fileCount)
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	s := ""
	if fileCount != 1 {
		s = "s"
	}
	for contentID := range errMap {
		logger.Verbose("Expected to see %d backup file%s on segment %d, but some were missing.", fileCount, s, contentID)
	}
	cluster.LogFatalError("Backup files missing", numErrors)
}

func (cluster *Cluster) VerifyBackupDirectoriesExistOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMapForCluster(func(contentID int) string {
		return fmt.Sprintf("test -d %s", cluster.GetDirForContent(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Directory %s missing or inaccessible for segment %d on host %s", cluster.GetDirForContent(contentID), contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Directories missing or inaccessible", numErrors)
}

func (cluster *Cluster) CreateBackupDirectoriesOnAllHosts() {
	logger.Verbose("Creating backup directories")
	commandMap := cluster.GenerateSSHCommandMapForCluster(func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Unable to create directory %s for segment %d on host %s", cluster.GetDirForContent(contentID), contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to create directories", numErrors)
}

func (cluster *Cluster) LogFatalError(errMessage string, numErrors int) {
	s := ""
	if numErrors != 1 {
		s = "s"
	}
	logger.Fatal(errors.Errorf("%s on %d segment%s. See %s for a complete list of segments with errors.", errMessage, numErrors, s, logger.GetLogFilePath()), "")
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.SegHostMap[contentID]
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	if cluster.IsUserSpecifiedBackupDir() {
		segDir := fmt.Sprintf("gpseg%d", contentID)
		return path.Join(cluster.UserSpecifiedBackupDir, segDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp)
	}
	return path.Join(cluster.SegDirMap[contentID], "backups", cluster.Timestamp[0:8], cluster.Timestamp)
}

func (cluster *Cluster) GetTableBackupFilePath(contentID int, tableOid uint32) string {
	templateFilePath := cluster.GetTableBackupFilePathForCopyCommand(tableOid)
	filePath := strings.Replace(templateFilePath, "<SEG_DATA_DIR>", cluster.SegDirMap[contentID], -1)
	return strings.Replace(filePath, "<SEGID>", strconv.Itoa(contentID), -1)
}

func (cluster *Cluster) GetTableBackupFilePathForCopyCommand(tableOid uint32) string {
	backupFilePath := fmt.Sprintf("gpbackup_<SEGID>_%s_%d", cluster.Timestamp, tableOid)
	if usingCompression {
		backupFilePath += compressionProgram.Extension
	}
	baseDir := "<SEG_DATA_DIR>"
	if cluster.IsUserSpecifiedBackupDir() {
		baseDir = path.Join(cluster.UserSpecifiedBackupDir, "gpseg<SEGID>")
	}
	return path.Join(baseDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp, backupFilePath)
}

/*
 * Backup and restore filename functions
 */

var metadataFilenameMap = map[string]string{
	"config":            "config.yaml",
	"global":            "global.sql",
	"predata":           "predata.sql",
	"postdata":          "postdata.sql",
	"statistics":        "statistics.sql",
	"table of contents": "toc.yaml",
	"report":            "report",
}

func (cluster *Cluster) GetBackupFilePath(filetype string) string {
	return path.Join(cluster.GetDirForContent(-1), fmt.Sprintf("gpbackup_%s_%s", cluster.Timestamp, metadataFilenameMap[filetype]))
}

func (cluster *Cluster) GetGlobalFilePath() string {
	return cluster.GetBackupFilePath("global")
}

func (cluster *Cluster) GetPredataFilePath() string {
	return cluster.GetBackupFilePath("predata")
}

func (cluster *Cluster) GetPostdataFilePath() string {
	return cluster.GetBackupFilePath("postdata")
}

func (cluster *Cluster) GetStatisticsFilePath() string {
	return cluster.GetBackupFilePath("statistics")
}

func (cluster *Cluster) GetTOCFilePath() string {
	return cluster.GetBackupFilePath("table of contents")
}

func (cluster *Cluster) GetReportFilePath() string {
	return cluster.GetBackupFilePath("report")
}

func (cluster *Cluster) GetConfigFilePath() string {
	return cluster.GetBackupFilePath("config")
}

func (cluster *Cluster) VerifyMetadataFilePaths(dataOnly bool, withStats bool, tableFiltered bool) {
	filetypes := []string{"config", "table of contents"}
	if !dataOnly {
		if !tableFiltered {
			filetypes = append(filetypes, []string{"global", "predata", "postdata"}...)
		} else {
			filetypes = append(filetypes, []string{"predata"}...)
		}
	}
	missing := false
	for _, filetype := range filetypes {
		filepath := cluster.GetBackupFilePath(filetype)
		if !FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := cluster.GetStatisticsFilePath()
		if !FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access statistics file %s", filepath)
			logger.Error(`Note that the "-with-stats" flag must be passed to gpbackup to generate a statistics file.`)
		}
	}
	if missing {
		logger.Fatal(errors.Errorf("One or more metadata files do not exist or are not readable."), "Cannot proceed with restore")
	}
}

/*
 * Helper functions
 */

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

func ConstructSSHCommand(host string, cmd string) []string {
	currentUser, _, _ := GetUserAndHostInfo()
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", currentUser, host), cmd}
}

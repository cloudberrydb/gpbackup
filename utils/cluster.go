package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"os/exec"
	"path"

	"github.com/pkg/errors"
)

type Cluster struct {
	ContentIDs             []int
	SegDirMap              map[int]string
	SegHostMap             map[int]string
	UserSpecifiedBackupDir string
	Timestamp              string
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
	return cluster
}

func (cluster *Cluster) IsUserSpecifiedBackupDir() bool {
	return cluster.UserSpecifiedBackupDir != ""
}

func ConstructSSHCommand(host string, cmd string) []string {
	currentUser, _, _ := GetUserAndHostInfo()
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", currentUser, host), cmd}
}

func (cluster *Cluster) GenerateSSHCommandMap(generateCommand func(int) string) map[int][]string {
	commandMap := make(map[int][]string, len(cluster.ContentIDs))
	for _, contentID := range cluster.ContentIDs {
		host := cluster.GetHostForContent(contentID)
		cmdStr := generateCommand(contentID)
		commandMap[contentID] = ConstructSSHCommand(host, cmdStr)
	}
	return commandMap
}

func (cluster *Cluster) GenerateFileVerificationCommandMap(fileCount int) map[int][]string {
	commandMap := cluster.GenerateSSHCommandMap(func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l | grep %d", cluster.GetDirForContent(contentID), fileCount)
	})
	delete(commandMap, -1) //The master host will have no backup files
	return commandMap
}

func (cluster *Cluster) ExecuteClusterCommand(commandMap map[int][]string) map[int]error {
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

func (cluster *Cluster) VerifyDirectoriesExistOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMap(func(contentID int) string {
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

func (cluster *Cluster) CreateDirectoriesOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMap(func(contentID int) string {
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
	logger.Fatal(errors.Errorf("%s on %d segment%s. See %s for a complete list of segments with errors.", errMessage, numErrors, s, logger.GetLogFileName()), "")
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

func (cluster *Cluster) GetTableMapFilePath() string {
	return path.Join(cluster.GetDirForContent(-1), fmt.Sprintf("gpbackup_%s_table_map", cluster.Timestamp))
}

func (cluster *Cluster) GetTableBackupFilePathForCopyCommand(tableOid uint32) string {
	backupFilename := fmt.Sprintf("gpbackup_<SEGID>_%s_%d", cluster.Timestamp, tableOid)
	baseDir := "<SEG_DATA_DIR>"
	if cluster.IsUserSpecifiedBackupDir() {
		baseDir = path.Join(cluster.UserSpecifiedBackupDir, "gpseg<SEGID>")
	}
	return path.Join(baseDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp, backupFilename)
}

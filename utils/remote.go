package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"os/exec"
	"path"
	"strings"

	"github.com/pkg/errors"
)

type Cluster struct {
	ContentIDs    []int
	SegDirMap     map[int]string
	SegHostMap    map[int]string
	RootBackupDir string
	Timestamp     string
}

func NewCluster(segConfigs []SegConfig, rootBackupDir string, timestamp string) Cluster {
	cluster := Cluster{}
	cluster.SegHostMap = make(map[int]string, 0)
	cluster.SegDirMap = make(map[int]string, 0)
	cluster.RootBackupDir = rootBackupDir
	cluster.Timestamp = timestamp
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.SegDirMap[seg.ContentID] = seg.DataDir
		cluster.SegHostMap[seg.ContentID] = seg.Hostname
	}
	return cluster
}

func ConstructSSHCommand(host string, cmd string) []string {
	currentUser, _, _ := GetUserAndHostInfo()
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", currentUser, host), cmd}
}

// This is a simple wrapper function to simplify testing
func execCommand(cmdArgs []string) error {
	logger.Debug("Executing command %s", strings.Join(cmdArgs, " "))
	_, err := exec.Command(cmdArgs[0], cmdArgs[1:]...).CombinedOutput()
	return err
}

func (cluster *Cluster) GetTableBackupFilePathForCopyCommand(tableOid uint32) string {
	if cluster.RootBackupDir != "" {
		return fmt.Sprintf("%s/gpbackup_<SEGID>_%s_%d", cluster.RootBackupDir, cluster.Timestamp, tableOid)
	}
	return fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_%s_%d", cluster.Timestamp, tableOid)
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
			errorList[index] = execCommand(segCommand)
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

func (cluster *Cluster) VerifyDirectoriesExistOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMap(func(contentID int) string {
		return fmt.Sprintf("test -d %s", cluster.GetDirForContent(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	if len(errMap) == 0 {
		return
	}
	numErrors := len(errMap)
	s := ""
	if numErrors != 1 {
		s = "s"
	}
	for contentID := range errMap {
		logger.Verbose("Directory %s missing or inaccessible on host %s", cluster.GetDirForContent(contentID), cluster.GetHostForContent(contentID))
	}
	logger.Fatal(errors.Errorf("Directories missing or inaccessible on %d host%s.  See %s for a complete list of hosts with errors.", numErrors, s, logger.GetLogFileName()), "")
}

func (cluster *Cluster) GenerateSSHCommandMap(gen func(int) string) map[int][]string {
	commandMap := make(map[int][]string, len(cluster.ContentIDs))
	for _, contentID := range cluster.ContentIDs {
		host := cluster.GetHostForContent(contentID)
		cmdStr := gen(contentID)
		commandMap[contentID] = ConstructSSHCommand(host, cmdStr)
	}
	return commandMap
}

func (cluster *Cluster) CreateDirectoriesOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMap(func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
	})

	errMap := cluster.ExecuteClusterCommand(commandMap)
	if len(errMap) == 0 {
		return
	}
	numErrors := len(errMap)
	s := ""
	if numErrors != 1 {
		s = "s"
	}
	for contentID := range errMap {
		logger.Verbose("Unable to create directory %s on host %s", cluster.GetDirForContent(contentID), cluster.GetHostForContent(contentID))
	}
	logger.Fatal(errors.Errorf("Unable to create directory on %d host%s.  See %s for a complete list of hosts with errors.", numErrors, s, logger.GetLogFileName()), "")
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.SegHostMap[contentID]
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	if cluster.RootBackupDir != "" {
		return path.Join(cluster.RootBackupDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp)
	}
	return path.Join(cluster.SegDirMap[contentID], "backups", cluster.Timestamp[0:8], cluster.Timestamp)
}

func (cluster *Cluster) GetTableMapFilePath() string {
	return fmt.Sprintf("%s/gpbackup_%s_table_map", cluster.GetDirForContent(-1), cluster.Timestamp)
}

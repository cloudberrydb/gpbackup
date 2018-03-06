package utils

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	yaml "gopkg.in/yaml.v2"
)

type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

func ReadPluginConfig(configFile string) *PluginConfig {
	config := &PluginConfig{}
	contents, err := operating.System.ReadFile(configFile)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, config)
	gplog.FatalOnError(err)
	return config
}

func (plugin *PluginConfig) Setup(cluster cluster.Cluster, configPath string) {
	plugin.CheckPluginExistsOnAllHosts(cluster)
	plugin.CopyPluginConfigToAllHosts(cluster, configPath)
	_, configFilename := filepath.Split(configPath)
	plugin.SetupPluginOnAllHosts(cluster, fmt.Sprintf("/tmp/%s", configFilename))
}

func (plugin *PluginConfig) BackupMetadata(filenamePath string, noFatal ...bool) {
	output, err := exec.Command(plugin.ExecutablePath, "backup_metadata", filenamePath).CombinedOutput()
	if err != nil {
		if len(noFatal) == 1 && noFatal[0] == true {
			gplog.Error(fmt.Sprintf("Plugin failed to process %s. %s", filenamePath, string(output)))
		} else {
			gplog.Fatal(err, string(output))
		}
	}
}

func (plugin *PluginConfig) RestoreMetadata(filenamePath string) {
	output, err := exec.Command(plugin.ExecutablePath, "restore_metadata", filenamePath).CombinedOutput()
	gplog.FatalOnError(err, string(output))
}

func (plugin *PluginConfig) CheckPluginExistsOnAllHosts(cluster cluster.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Checking that plugin exists on all segment hosts", func(contentID int) string {
		return fmt.Sprintf("test -x %s", plugin.ExecutablePath)
	})
	cluster.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) SetupPluginOnAllHosts(cluster cluster.Cluster, configPath string) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Running plugin setup on all segment hosts", func(contentID int) string {
		return fmt.Sprintf("%s setup_plugin %s", plugin.ExecutablePath, configPath)
	})
	cluster.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) CleanupPluginOnAllHosts(cluster cluster.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Running plugin cleanup on all segment hosts", func(contentID int) string {
		return fmt.Sprintf("%s cleanup_plugin", plugin.ExecutablePath)
	})
	cluster.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath)
	}, true)
}

func (plugin *PluginConfig) CopyPluginConfigToAllHosts(cluster cluster.Cluster, configPath string) {
	hostnameSet := NewIncludeSet([]string{})
	for _, hostname := range cluster.SegHostMap {
		hostnameSet.Add(hostname)
	}
	hostnameSet.Delete(cluster.SegHostMap[-1])
	hostnameStr := ""
	for hostname := range hostnameSet.Set {
		hostnameStr += fmt.Sprintf("-h %s ", hostname)
	}
	_, configFilename := filepath.Split(configPath)
	output, err := cluster.ExecuteLocalCommand(fmt.Sprintf("rsync %s /tmp/.", configPath))
	gplog.FatalOnError(err, output)
	if hostnameStr != "" {
		output, err = cluster.ExecuteLocalCommand(fmt.Sprintf("gpscp %s /tmp/%s /tmp/%s", hostnameStr, configFilename, configFilename))
		gplog.FatalOnError(err, output)
	}
}

func (plugin *PluginConfig) BackupSegmentTOCs(cluster cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, fpInfo.Timestamp)
		return fmt.Sprintf("%s backup_metadata %s/%s", plugin.ExecutablePath, fpInfo.GetDirForContent(contentID), tocFilename)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}

func (plugin *PluginConfig) RestoreSegmentTOCs(cluster cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, fpInfo.Timestamp)
		return fmt.Sprintf("%s restore_metadata %s/%s", plugin.ExecutablePath, fpInfo.GetDirForContent(contentID), tocFilename)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}

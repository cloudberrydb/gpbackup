package utils

import (
	"fmt"
	"os"
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
	config.ExecutablePath = os.ExpandEnv(config.ExecutablePath)
	ValidateFullPath(config.ExecutablePath)
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

func (plugin *PluginConfig) CheckPluginExistsOnAllHosts(c cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Checking that plugin exists on all hosts", func(contentID int) string {
		return fmt.Sprintf("test -x %s", plugin.ExecutablePath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to execute plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) SetupPluginOnAllHosts(c cluster.Cluster, configPath string) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin setup on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s setup_plugin %s", plugin.ExecutablePath, configPath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to setup plugin %s", plugin.ExecutablePath)
	})
}

func (plugin *PluginConfig) CleanupPluginOnAllHosts(c cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Running plugin cleanup on all hosts", func(contentID int) string {
		return fmt.Sprintf("%s cleanup_plugin", plugin.ExecutablePath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath), func(contentID int) string {
		return fmt.Sprintf("Unable to cleanup plugin %s", plugin.ExecutablePath)
	}, true)
}

func (plugin *PluginConfig) CopyPluginConfigToAllHosts(c cluster.Cluster, configPath string) {
	remoteOutput := c.GenerateAndExecuteCommand("Copying plugin config to all hosts", func(contentID int) string {
		return fmt.Sprintf("rsync %s:%s /tmp/.", c.GetHostForContent(-1), configPath)
	}, cluster.ON_HOSTS_AND_MASTER)
	c.CheckClusterError(remoteOutput, "Unable to copy plugin config", func(contentID int) string {
		return "Unable to copy plugin config"
	})
}

func (plugin *PluginConfig) BackupSegmentTOCs(c cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, fpInfo.Timestamp)
		return fmt.Sprintf("%s backup_metadata %s/%s", plugin.ExecutablePath, fpInfo.GetDirForContent(contentID), tocFilename)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}

func (plugin *PluginConfig) RestoreSegmentTOCs(c cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Processing segment TOC files with plugin", func(contentID int) string {
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, fpInfo.Timestamp)
		return fmt.Sprintf("%s restore_metadata %s/%s", plugin.ExecutablePath, fpInfo.GetDirForContent(contentID), tocFilename)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to process segment TOC files using plugin", func(contentID int) string {
		return fmt.Sprintf("Unable to process segment TOC files using plugin")
	})
}

package utils_test

import (
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/plugin tests", func() {
	stdOut := make(map[int]string, 1)
	var testCluster *cluster.Cluster
	var executor testhelper.TestExecutor
	var subject utils.PluginConfig

	BeforeEach(func() {
		subject = utils.PluginConfig{
			ExecutablePath: "myPlugin",
			ConfigPath:     "/tmp/my_plugin_config.yaml",
		}
		executor = testhelper.TestExecutor{
			ClusterOutput: &cluster.RemoteOutput{
				Stdouts: stdOut,
			},
		}
		stdOut[0] = utils.RequiredPluginVersion // this is a successful result
		testCluster = &cluster.Cluster{
			ContentIDs: []int{-1, 0, 1},
			Executor:   &executor,
			Segments: map[int]cluster.SegConfig{
				-1: {DataDir: "/data/gpseg-1", Hostname: "master", Port: 100},
				0:  {DataDir: "/data/gpseg0", Hostname: "segment1", Port: 101},
				1:  {DataDir: "/data/gpseg1", Hostname: "segment2", Port: 102},
			},
		}
	})
	AfterEach(func() {
		operating.System = operating.InitializeSystemFunctions()
	})
	Describe("gpbackup plugin interface generates the correct", func() {
		It("api command", func() {
			operating.System.Getenv = func(key string) string {
				return "my/install/dir"
			}

			subject.CheckPluginExistsOnAllHosts(testCluster)

			allCommands := executor.ClusterCommands[0] // only one set of commands was issued
			expectedCommand := "source my/install/dir/greenplum_path.sh && myPlugin plugin_api_version"
			for _, contentID := range testCluster.ContentIDs {
				cmd := allCommands[contentID]
				Expect(cmd[len(cmd)-1]).To(Equal(expectedCommand))
			}
		})
	})
	Describe("plugin config", func() {
		It("successfully copies to all hosts", func() {
			testConfigPath := "/tmp/my_plugin_config.yaml"
			testConfigContents := `
executablepath: /tmp/fake_path
options:
    field1: 12
    field2: hello
    field3: 567
`
			file := iohelper.MustOpenFileForWriting(testConfigPath)
			_, err := file.Write([]byte(testConfigContents))
			gplog.FatalOnError(err)
			myconfig, err := utils.ReadPluginConfig(testConfigPath)
			subject.Options = myconfig.Options // hack to get options set up in test situation

			subject.CopyPluginConfigToAllHosts(testCluster)

			Expect(executor.NumExecutions).To(Equal(1))
			cc := executor.ClusterCommands[0]
			Expect(len(cc)).To(Equal(3))
			Expect(cc[-1][2]).To(Equal("scp /tmp/my_plugin_config.yaml_-1 master:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_-1"))
			Expect(cc[0][2]).To(Equal("scp /tmp/my_plugin_config.yaml_0 segment1:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_0"))
			Expect(cc[1][2]).To(Equal("scp /tmp/my_plugin_config.yaml_1 segment2:/tmp/my_plugin_config.yaml; rm /tmp/my_plugin_config.yaml_1"))

			// check contents
			contents := strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_-1"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"100\""))
			contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_0"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"101\""))
			contents = strings.Join(iohelper.MustReadLinesFromFile("/tmp/my_plugin_config.yaml_1"), "\n")
			Expect(contents).To(ContainSubstring("\n  pgport: \"102\""))
		})
	})
	Describe("version validation", func() {
		When("version is equal to requirement", func() {
			It("succeeds", func() {
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version is greater than requirement", func() {
			It("succeeds", func() {
				// add one to whatever the current required version might be
				version, _ := semver.Make(utils.RequiredPluginVersion)
				greater, _ := semver.Make(strconv.Itoa(int(version.Major)+1) + ".0.0")
				executor.ClusterOutput.Stdouts[0] = greater.String()

				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version is too low", func() {
			It("panics with message", func() {
				executor.ClusterOutput.Stdouts[0] = "0.2.0"

				defer testhelper.ShouldPanicWithMessage("Plugin API version incorrect")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version cannot be parsed", func() {
			It("panics with message", func() {
				executor.ClusterOutput.Stdouts[0] = "foo"

				defer testhelper.ShouldPanicWithMessage("Unable to parse plugin API version")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
		When("version command fails", func() {
			It("panics with message", func() {
				subject.ExecutablePath = "myFailingPlugin"
				executor.ClusterOutput.NumErrors = 1

				defer testhelper.ShouldPanicWithMessage("Unable to execute plugin myFailingPlugin")
				subject.CheckPluginExistsOnAllHosts(testCluster)
			})
		})
	})
})

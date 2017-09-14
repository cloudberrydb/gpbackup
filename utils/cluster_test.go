package utils_test

import (
	"database/sql/driver"
	"fmt"
	"os"
	"os/user"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/cluster tests", func() {
	masterSeg := utils.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := utils.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := utils.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	remoteSegTwo := utils.SegConfig{ContentID: 2, Hostname: "remotehost2", DataDir: "/data/gpseg2"}
	var (
		testCluster  utils.Cluster
		testExecutor *testutils.TestExecutor
	)

	BeforeEach(func() {
		utils.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		utils.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testutils.TestExecutor{}
		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) { return nil, nil }
		testCluster = utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne, remoteSegOne}, "", "20170101010101")
		testCluster.Executor = testExecutor
	})
	Describe("ConstructSSHCommand", func() {
		It("constructs an ssh command", func() {
			cmd := utils.ConstructSSHCommand("some-host", "ls")
			Expect(cmd).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@some-host", "ls"}))
		})
	})
	Describe("GetSegmentConfiguration", func() {
		header := []string{"contentid", "hostname", "datadir"}
		localSegOne := []driver.Value{"0", "localhost", "/data/gpseg0"}
		localSegTwo := []driver.Value{"1", "localhost", "/data/gpseg1"}
		remoteSegOne := []driver.Value{"2", "remotehost", "/data/gpseg2"}

		It("returns a configuration for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
	})
	Describe("GenerateSSHCommandMap", func() {
		It("Returns a map of ssh commands for the master, including master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
		})
		It("Returns a map of ssh commands for the master, excluding master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(0))
		})
		It("Returns a map of ssh commands for one segment, including master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{remoteSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for one segment, excluding master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{remoteSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for two segments on the same host, including master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(2))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
		It("Returns a map of ssh commands for two segments on the same host, excluding master", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
		It("Returns a map of ssh commands for three segments on different hosts", func() {
			cluster := utils.NewCluster([]utils.SegConfig{localSegOne, remoteSegOne, remoteSegTwo}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMap(false, func(contentID int) string {
				return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
			})
			Expect(len(commandMap)).To(Equal(3))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "mkdir -p /data/gpseg0/backups/20170101/20170101010101"}))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "mkdir -p /data/gpseg1/backups/20170101/20170101010101"}))
			Expect(commandMap[2]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost2", "mkdir -p /data/gpseg2/backups/20170101/20170101010101"}))
		})
	})
	Describe("GenerateSSHCommandMapForCluster", func() {
		It("includes master in the command map", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMapForCluster(func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(2))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
	})
	Describe("GenerateSSHCommandMapForSegments", func() {
		It("excludes master from the command map", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateSSHCommandMapForSegments(func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
	})
	Describe("GenerateFileVerificationCommandMap", func() {
		It("creates a command map for segments only", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne, remoteSegOne}, "", "20170101010101")
			commandMap := cluster.GenerateFileVerificationCommandMap(13)

			Expect(len(commandMap)).To(Equal(2))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "find /data/gpseg0/backups/20170101/20170101010101 -type f | wc -l | grep 13"}))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "find /data/gpseg1/backups/20170101/20170101010101 -type f | wc -l | grep 13"}))
		})
	})
	Describe("ExecuteLocalCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/backup_and_restore_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/backup_and_restore_test")
		})
		It("runs the specified command", func() {
			cluster := utils.Cluster{}
			commandStr := "touch /tmp/backup_and_restore_test/foo"
			cluster.Executor = &utils.GPDBExecutor{}
			cluster.ExecuteLocalCommand(commandStr)

			testutils.ExpectPathToExist("/tmp/backup_and_restore_test/foo")
		})
		It("returns any error generated by the specified command", func() {
			cluster := utils.Cluster{}
			commandStr := "some-non-existant-command /tmp/backup_and_restore_test/foo"
			cluster.Executor = &utils.GPDBExecutor{}
			err := cluster.ExecuteLocalCommand(commandStr)

			Expect(err.Error()).To(Equal("exit status 127"))
		})
	})
	Describe("ExecuteClusterCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/backup_and_restore_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/backup_and_restore_test")
		})
		It("runs commands specified by command map", func() {
			cluster := utils.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/backup_and_restore_test/foo"},
				0:  {"touch", "/tmp/backup_and_restore_test/baz"},
			}
			cluster.Executor = &utils.GPDBExecutor{}
			cluster.ExecuteClusterCommand(commandMap)

			testutils.ExpectPathToExist("/tmp/backup_and_restore_test/foo")
			testutils.ExpectPathToExist("/tmp/backup_and_restore_test/baz")
		})
		It("returns any errors generated by any of the commands", func() {
			cluster := utils.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/backup_and_restore_test/foo"},
				0:  {"some-non-existant-command"},
			}
			cluster.Executor = &utils.GPDBExecutor{}
			errMap := cluster.ExecuteClusterCommand(commandMap)

			testutils.ExpectPathToExist("/tmp/backup_and_restore_test/foo")
			Expect(len(errMap)).To(Equal(1))
			Expect(errMap[0].Error()).To(Equal("exec: \"some-non-existant-command\": executable file not found in $PATH"))
		})
	})
	Describe("LogFatalError", func() {
		It("logs an error for 1 segment", func() {
			cluster := utils.NewCluster(nil, "", "20170101010101")
			defer testutils.ShouldPanicWithMessage("Error occurred on 1 segment. See gbytes.Buffer for a complete list of segments with errors.")
			cluster.LogFatalError("Error occurred", 1)
		})
		It("logs an error for more than 1 segment", func() {
			cluster := utils.NewCluster(nil, "", "20170101010101")
			defer testutils.ShouldPanicWithMessage("Error occurred on 2 segments. See gbytes.Buffer for a complete list of segments with errors.")
			cluster.LogFatalError("Error occurred", 2)
		})
	})
	Describe("cluster setup and accessor functions", func() {
		masterSeg := utils.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
		localSegOne := utils.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
		localSegTwo := utils.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "/data/gpseg1"}
		remoteSegTwo := utils.SegConfig{ContentID: 1, Hostname: "remotehost", DataDir: "/data/gpseg1"}
		It("returns content dir for a single-host, single-segment nodes", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne}, "", "20170101010101")
			Expect(len(cluster.GetContentList())).To(Equal(2))
			Expect(cluster.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
		})
		It("sets up the configuration for a single-host, multi-segment cluster", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne, localSegTwo}, "", "20170101010101")
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.GetDirForContent(1)).To(Equal("/data/gpseg1/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(1)).To(Equal("localhost"))
		})
		It("sets up the configuration for a multi-host, multi-segment cluster", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne, remoteSegTwo}, "", "20170101010101")
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.GetDirForContent(1)).To(Equal("/data/gpseg1/backups/20170101/20170101010101"))
			Expect(cluster.GetHostForContent(1)).To(Equal("remotehost"))
		})
		It("returns the content directory based on the user specified path", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "/foo/bar", "20170101010101")
			Expect(cluster.GetDirForContent(-1)).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101"))
		})
		It("Panics if lock file exists for current timestamp", func() {
			utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return nil, errors.New("file does not exist")
			}
			defer testutils.ShouldPanicWithMessage("A backup with timestamp 20170101010101 is already in progress. Wait 1 second and try the backup again.")
			utils.NewCluster([]utils.SegConfig{masterSeg}, "/foo/bar", "20170101010101")
		})
	})
	Describe("GetTableBackupFilePathForCopyCommand()", func() {
		It("returns table file path for copy command", func() {
			cluster := utils.NewCluster(nil, "", "20170101010101")
			Expect(cluster.GetTableBackupFilePathForCopyCommand(1234)).To(Equal("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234"))
		})
		It("returns table file path for copy command based on user specifed path", func() {
			cluster := utils.NewCluster(nil, "/foo/bar", "20170101010101")
			Expect(cluster.GetTableBackupFilePathForCopyCommand(1234)).To(Equal("/foo/bar/gpseg<SEGID>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234"))
		})
	})
	Describe("GetTableMapFilePath()", func() {
		It("returns table map file path", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "", "20170101010101")
			Expect(cluster.GetTableMapFilePath()).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_table_map"))
		})
		It("returns table map file path based on user specified path", func() {
			cluster := utils.NewCluster(nil, "/foo/bar", "20170101010101")
			Expect(cluster.GetTableMapFilePath()).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_table_map"))
		})
	})
	Describe("GetReportFilePath", func() {
		It("returns report file path", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "", "20170101010101")
			Expect(cluster.GetReportFilePath()).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_report"))
		})
		It("returns report file path based on user specified path", func() {
			cluster := utils.NewCluster(nil, "/foo/bar", "20170101010101")
			Expect(cluster.GetReportFilePath()).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_report"))
		})
	})
	Describe("GetTableBackupFilePath", func() {
		It("returns table file path", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "", "20170101010101")
			Expect(cluster.GetTableBackupFilePath(-1, 1234)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101_1234"))
		})
		It("returns table file path based on user specified path", func() {
			cluster := utils.NewCluster([]utils.SegConfig{masterSeg}, "/foo/bar", "20170101010101")
			Expect(cluster.GetTableBackupFilePath(-1, 1234)).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101_1234"))
		})
	})
	Describe("VerifyBackupFileCountOnSegments", func() {
		It("successfully verifies all backup file counts", func() {
			testCluster.VerifyBackupFileCountOnSegments(2)
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if it cannot verify all backup file counts", func() {
			testExecutor.ClusterError = map[int]error{
				0: errors.Errorf("exit status 1"),
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Backup files missing on 2 segments")
			testCluster.VerifyBackupFileCountOnSegments(2)
		})
		It("panics if it cannot verify some backup file counts", func() {
			testExecutor.ClusterError = map[int]error{
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Backup files missing on 1 segment")
			testCluster.VerifyBackupFileCountOnSegments(2)
		})
	})
	Describe("VerifyBackupDirectoriesExistOnAllHosts", func() {
		It("successfully verifies all directories", func() {
			testCluster.VerifyBackupDirectoriesExistOnAllHosts()
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if it cannot verify all directories", func() {
			testExecutor.ClusterError = map[int]error{
				0: errors.Errorf("exit status 1"),
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Directories missing or inaccessible on 2 segments")
			testCluster.VerifyBackupDirectoriesExistOnAllHosts()
		})
		It("panics if it cannot verify some directories", func() {
			testExecutor.ClusterError = map[int]error{
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Directories missing or inaccessible on 1 segment")
			testCluster.VerifyBackupDirectoriesExistOnAllHosts()
		})
	})
	Describe("CreateBackupDirectoriesOnAllHosts", func() {
		It("successfully creates all directories", func() {
			testCluster.CreateBackupDirectoriesOnAllHosts()
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if it cannot create all directories", func() {
			testExecutor.ClusterError = map[int]error{
				0: errors.Errorf("exit status 1"),
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Unable to create directories on 2 segments")
			testCluster.CreateBackupDirectoriesOnAllHosts()
		})
		It("panics if it cannot create some directories", func() {
			testExecutor.ClusterError = map[int]error{
				1: errors.Errorf("exit status 1"),
			}
			testCluster.Executor = testExecutor
			defer testutils.ShouldPanicWithMessage("Unable to create directories on 1 segment")
			testCluster.CreateBackupDirectoriesOnAllHosts()
		})
	})
})

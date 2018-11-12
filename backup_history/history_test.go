package backup_history_test

import (
	"errors"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v2"
)

var _ bool = Describe("backup/history tests", func() {
	var testConfig1, testConfig2, testConfig3 backup_history.BackupConfig
	var historyFilePath = "/tmp/history_file.yaml"

	BeforeEach(func() {
		testConfig1 = backup_history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp1",
		}
		testConfig2 = backup_history.BackupConfig{
			DatabaseName:     "testdb2",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp2",
		}
		testConfig3 = backup_history.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp3",
		}
	})
	Describe("NewHistory", func() {
		It("creates a history object with entries from the file when history file exists", func() {
			historyWithEntries := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig1, testConfig2}}
			historyFileContents, _ := yaml.Marshal(historyWithEntries)
			fileHandle := iohelper.MustOpenFileForWriting(historyFilePath)
			fileHandle.Write(historyFileContents)
			fileHandle.Close()
			defer os.Remove(historyFilePath)

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
		})
		Context("fatals when", func() {
			BeforeEach(func() {
				operating.System.Stat = func(string) (os.FileInfo, error) { return nil, nil }
				operating.System.OpenFileRead = func(string, int, os.FileMode) (operating.ReadCloserAt, error) { return nil, nil }
			})
			AfterEach(func() {
				operating.System = operating.InitializeSystemFunctions()
			})
			It("gpbackup_history.yaml can't be read", func() {
				operating.System.ReadFile = func(string) ([]byte, error) { return nil, errors.New("read error") }

				_, err := backup_history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("read error"))
			})
			It("gpbackup_history.yaml is an invalid format", func() {
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte("not yaml"), nil }

				_, err := backup_history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not yaml"))
			})
			It("NewHistory returns an empty History", func() {
				backup.SetFPInfo(backup_filepath.FilePathInfo{UserSpecifiedBackupDir: "/tmp", UserSpecifiedSegPrefix: "/test-prefix"})
				backup.SetReport(&utils.Report{})
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte(""), nil }

				history, err := backup_history.NewHistory("/tempfile")
				Expect(err).ToNot(HaveOccurred())
				Expect(history).To(Equal(&backup_history.History{BackupConfigs: make([]backup_history.BackupConfig, 0)}))

			})
		})

	})
	Describe("AddBackupConfig", func() {
		It("adds the most recent history entry and keeps the list sorted", func() {
			testHistory := backup_history.History{BackupConfigs: []backup_history.BackupConfig{testConfig3, testConfig1}}

			testHistory.AddBackupConfig(&testConfig2)

			expectedHistory := backup_history.History{BackupConfigs: []backup_history.BackupConfig{testConfig3, testConfig2, testConfig1}}
			structmatcher.ExpectStructsToMatch(&expectedHistory, &testHistory)
		})
	})
})

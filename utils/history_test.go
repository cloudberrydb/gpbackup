package utils_test

import (
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"os"
)

var _ bool = Describe("backup/history tests", func() {
	var testEntry1, testEntry2, testEntry3 utils.HistoryEntry
	var historyFilePath = "/tmp/history_file.yaml"

	BeforeEach(func() {
		testEntry1 = utils.HistoryEntry{
			Dbname:           "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			Timestamp:        "timestamp1",
		}
		testEntry2 = utils.HistoryEntry{
			Dbname:           "testdb2",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			Timestamp:        "timestamp2",
		}
		testEntry3 = utils.HistoryEntry{
			Dbname:           "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			Timestamp:        "timestamp3",
		}
	})
	Describe("NewHistory", func() {
		Context("history file doesn't exist", func() {
			It("creates an empty history object", func() {
				operating.System.Stat = func(name string) (os.FileInfo, error) { return nil, errors.New("file does not exist") }

				resultHistory := utils.NewHistory(historyFilePath)

				structmatcher.ExpectStructsToMatch(&utils.History{Entries: []utils.HistoryEntry{}}, resultHistory)

				operating.System.Stat = os.Stat
			})
		})
		Context("history file exists", func() {
			It("creates a history object with entries from the file", func() {
				historyWithEntries := utils.History{Entries: []utils.HistoryEntry{testEntry1, testEntry2}}
				historyFileContents, _ := yaml.Marshal(historyWithEntries)
				fileHandle := iohelper.MustOpenFileForWriting(historyFilePath)
				fileHandle.Write(historyFileContents)
				fileHandle.Close()
				defer os.Remove(historyFilePath)

				resultHistory := utils.NewHistory(historyFilePath)

				structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
			})
		})
	})
	Describe("HistoryEntryFromFlagSet", func() {
		It("can convert a FlagSet to a HistoryEntry", func() {
			backupCmdFlags.Set(backup.DBNAME, "testdb1")
			backupCmdFlags.Set(backup.INCLUDE_RELATION, "testschema.testtable1,testschema.testtable2")
			resultHistoryEntry := utils.HistoryEntryFromFlagSet("timestamp1", backupCmdFlags)

			structmatcher.ExpectStructsToMatch(&testEntry1, resultHistoryEntry)
		})
	})
	Describe("AddHistoryEntry", func() {
		It("adds the most recent history entry and keeps the list sorted", func() {
			testHistory := utils.History{Entries: []utils.HistoryEntry{testEntry3, testEntry1}}

			testHistory.AddHistoryEntry(&testEntry2)

			expectedHistory := utils.History{Entries: []utils.HistoryEntry{testEntry3, testEntry2, testEntry1}}
			structmatcher.ExpectStructsToMatch(&expectedHistory, &testHistory)
		})
	})
})

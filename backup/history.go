package backup

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/nightlyone/lockfile"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"sort"
	"time"
)

type HistoryEntry struct {
	BackupDir         string   `yaml:"backup-dir"`
	DataOnly          bool     `yaml:"data-only"`
	Dbname            string   `yaml:"dbname"`
	ExcludeRelations  []string `yaml:"exclude-table"`
	ExcludeSchemas    []string `yaml:"exclude-schema"`
	IncludeRelations  []string `yaml:"include-table"`
	IncludeSchemas    []string `yaml:"include-schema"`
	LeafPartitionData bool     `yaml:"leaf-partition-data"`
	MetadataOnly      bool     `yaml:"metadata-only"`
	NoCompression     bool     `yaml:"no-compression"`
	PluginConfigFile  string   `yaml:"plugin-config"`
	SingleDataFile    bool     `yaml:"single-data-file"`
	Timestamp         string   `yaml:"timestamp"`
}

type History struct {
	Entries []HistoryEntry
}

func NewHistory(filename string) *History {
	history := &History{}
	if historyFileExists := iohelper.FileExistsAndIsReadable(filename); historyFileExists {
		contents, err := operating.System.ReadFile(filename)

		gplog.FatalOnError(err)
		err = yaml.Unmarshal(contents, history)
		gplog.FatalOnError(err)
	}
	return history
}

func HistoryEntryFromFlagSet(timestamp string, flagSet *pflag.FlagSet) *HistoryEntry {
	viperInstance := viper.New()
	viperInstance.SetConfigType("yaml")
	_ = viperInstance.BindPFlags(flagSet)
	allSettings := viperInstance.AllSettings()
	out, err := yaml.Marshal(allSettings)
	gplog.FatalOnError(err)

	historyEntry := HistoryEntry{}
	err = yaml.Unmarshal(out, &historyEntry)
	gplog.FatalOnError(err)
	historyEntry.Timestamp = timestamp

	return &historyEntry
}

func (history *History) AddHistoryEntry(historyEntry *HistoryEntry) {
	history.Entries = append(history.Entries, *historyEntry)
	sort.Slice(history.Entries, func(i, j int) bool {
		return history.Entries[i].Timestamp > history.Entries[j].Timestamp
	})
}

func WriteBackupHistory(historyFilePath string, historyEntry *HistoryEntry) {
	lock := lockHistoryFile()

	history := NewHistory(historyFilePath)
	history.AddHistoryEntry(historyEntry)
	history.writeToFileAndMakeReadOnly(historyFilePath)

	_ = lock.Unlock()
}

func lockHistoryFile() lockfile.Lockfile {
	lock, err := lockfile.New("/tmp/backup_history.yaml.lck")
	gplog.FatalOnError(err)
	err = lock.TryLock()
	for err != nil {
		time.Sleep(50 * time.Millisecond)
		err = lock.TryLock()
	}
	return lock
}

func (history *History) writeToFileAndMakeReadOnly(filename string) {
	_, err := operating.System.Stat(filename)
	fileExists := err == nil
	if fileExists {
		err = operating.System.Chmod(filename, 0644)
		gplog.FatalOnError(err)
	}
	historyFile := iohelper.MustOpenFileForWriting(filename)
	historyFileContents, err := yaml.Marshal(history)
	gplog.FatalOnError(err)
	utils.MustPrintBytes(historyFile, historyFileContents)
	err = operating.System.Chmod(filename, 0444)
	gplog.FatalOnError(err)
}

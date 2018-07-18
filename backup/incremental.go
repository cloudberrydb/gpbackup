package backup

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func FilterTablesForIncremental(lastBackupTOC, currentTOC *utils.TOC, tables []Relation) []Relation {
	var filteredTables []Relation
	for _, table := range tables {
		currentAOEntry, isAOTable := currentTOC.IncrementalMetadata.AO[table.ToString()]
		if !isAOTable {
			filteredTables = append(filteredTables, table)
			continue
		}
		previousAOEntry := lastBackupTOC.IncrementalMetadata.AO[table.ToString()]

		if previousAOEntry.Modcount != currentAOEntry.Modcount || previousAOEntry.LastDDLTimestamp != currentAOEntry.LastDDLTimestamp {
			filteredTables = append(filteredTables, table)
		}
	}

	return filteredTables
}

func GetLatestMatchingBackupTimestamp() string {
	if fromTimestamp := MustGetFlagString(FROM_TIMESTAMP); fromTimestamp != "" {
		return fromTimestamp
	}

	history := NewHistory(globalFPInfo.GetBackupHistoryFilePath())
	latestMatchingBackupHistoryEntry := GetLatestMatchingHistoryEntry(history)
	if latestMatchingBackupHistoryEntry == nil {
		gplog.FatalOnError(errors.Errorf("There was no matching previous backup found with the flags provided. " +
			"Please take a full backup."))
	}

	return latestMatchingBackupHistoryEntry.Timestamp
}

func GetLatestMatchingHistoryEntry(history *History) *HistoryEntry {
	for _, historyEntry := range history.Entries {
		if historyEntry.BackupDir == MustGetFlagString(BACKUP_DIR) &&
			historyEntry.Dbname == MustGetFlagString(DBNAME) &&
			historyEntry.LeafPartitionData == MustGetFlagBool(LEAF_PARTITION_DATA) &&
			historyEntry.PluginConfigFile == MustGetFlagString(PLUGIN_CONFIG) &&
			historyEntry.SingleDataFile == MustGetFlagBool(SINGLE_DATA_FILE) &&
			historyEntry.NoCompression == MustGetFlagBool(NO_COMPRESSION) &&
			utils.NewIncludeSet(historyEntry.IncludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(INCLUDE_RELATION))) &&
			utils.NewIncludeSet(historyEntry.IncludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(INCLUDE_SCHEMA))) &&
			utils.NewIncludeSet(historyEntry.ExcludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(EXCLUDE_RELATION))) &&
			utils.NewIncludeSet(historyEntry.ExcludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(EXCLUDE_SCHEMA))) {
			return &historyEntry
		}
	}

	return nil
}

func GetLastBackupTOC(lastBackupTimestamp string) *utils.TOC {
	lastBackupFPInfo := utils.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
		lastBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix)
	lastBackupTOCFilePath := lastBackupFPInfo.GetTOCFilePath()
	lastBackupTOC := utils.NewTOC(lastBackupTOCFilePath)

	return lastBackupTOC
}

func GetLastBackupRestorePlan(lastBackupTimestamp string) []utils.RestorePlanEntry {
	lastBackupFPInfo := utils.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
		lastBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix)
	lastBackupConfigFile := lastBackupFPInfo.GetConfigFilePath()
	lastBackupRestorePlan := utils.ReadConfigFile(lastBackupConfigFile).RestorePlan

	return lastBackupRestorePlan
}

func PopulateRestorePlan(changedTables []Relation,
	restorePlan []utils.RestorePlanEntry, allTables []Relation) []utils.RestorePlanEntry {
	currBackupRestorePlanEntry := utils.RestorePlanEntry{
		Timestamp: globalFPInfo.Timestamp,
		TableFQNs: make([]string, 0, len(changedTables)),
	}

	for _, changedTable := range changedTables {
		changedTableFQN := changedTable.ToString()
		currBackupRestorePlanEntry.TableFQNs = append(currBackupRestorePlanEntry.TableFQNs, changedTableFQN)
	}

	changedTableFQNs := make(map[string]bool)
	for _, changedTable := range changedTables {
		changedTableFQN := changedTable.ToString()
		changedTableFQNs[changedTableFQN] = true
	}

	allTableFQNs := make(map[string]bool)
	for _, table := range allTables {
		tableFQN := table.ToString()
		allTableFQNs[tableFQN] = true
	}

	//Removing filtered table FQNs for the current backup from entries with previous timestamps
	for i, restorePlanEntry := range restorePlan {
		tableFQNs := make([]string, 0)
		for _, tableFQN := range restorePlanEntry.TableFQNs {
			if !changedTableFQNs[tableFQN] && allTableFQNs[tableFQN] {
				tableFQNs = append(tableFQNs, tableFQN)
			}
		}
		restorePlan[i].TableFQNs = tableFQNs
	}
	restorePlan = append(restorePlan, currBackupRestorePlanEntry)

	return restorePlan
}

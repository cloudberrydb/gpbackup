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
	if fromTimestamp := MustGetFlagString(utils.FROM_TIMESTAMP); fromTimestamp != "" {
		return fromTimestamp
	}

	history := utils.NewHistory(globalFPInfo.GetBackupHistoryFilePath())
	latestMatchingBackupHistoryEntry := GetLatestMatchingHistoryEntry(history)
	if latestMatchingBackupHistoryEntry == nil {
		gplog.FatalOnError(errors.Errorf("There was no matching previous backup found with the flags provided. " +
			"Please take a full backup."))
	}

	return latestMatchingBackupHistoryEntry.Timestamp
}

func GetLatestMatchingHistoryEntry(history *utils.History) *utils.HistoryEntry {
	for _, historyEntry := range history.Entries {
		if historyEntry.BackupDir == MustGetFlagString(utils.BACKUP_DIR) &&
			historyEntry.Dbname == MustGetFlagString(utils.DBNAME) &&
			historyEntry.LeafPartitionData == MustGetFlagBool(utils.LEAF_PARTITION_DATA) &&
			historyEntry.PluginConfigFile == MustGetFlagString(utils.PLUGIN_CONFIG) &&
			historyEntry.SingleDataFile == MustGetFlagBool(utils.SINGLE_DATA_FILE) &&
			historyEntry.NoCompression == MustGetFlagBool(utils.NO_COMPRESSION) &&
			utils.NewIncludeSet(historyEntry.IncludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.INCLUDE_RELATION))) &&
			utils.NewIncludeSet(historyEntry.IncludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA))) &&
			utils.NewIncludeSet(historyEntry.ExcludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.EXCLUDE_RELATION))) &&
			utils.NewIncludeSet(historyEntry.ExcludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA))) {
			return &historyEntry
		}
	}

	return nil
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

package backup

import (
	"github.com/greenplum-db/gpbackup/utils"
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

func GetLastBackupTimestamp() string {
	if fromTimestamp := MustGetFlagString(FROM_TIMESTAMP); fromTimestamp != "" {
		return fromTimestamp
	}
	return ""
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

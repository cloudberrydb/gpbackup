package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(postdataFile *utils.FileWithByteCount, toc *utils.TOC, indexes []QuerySimpleDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := postdataFile.ByteCount
		postdataFile.MustPrintf("\n\n%s;", index.Def)
		if index.Tablespace != "" {
			postdataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", index.Name, index.Tablespace)
		}
		PrintObjectMetadata(postdataFile, indexMetadata[index.Oid], index.Name, "INDEX")
		toc.AddMetadataEntry(index.OwningSchema, index.Name, "INDEX", start, postdataFile)
	}
}

func PrintCreateRuleStatements(postdataFile *utils.FileWithByteCount, toc *utils.TOC, rules []QuerySimpleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := postdataFile.ByteCount
		postdataFile.MustPrintf("\n\n%s", rule.Def)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(postdataFile, ruleMetadata[rule.Oid], rule.Name, "RULE", tableFQN)
		toc.AddMetadataEntry(rule.OwningSchema, rule.Name, "RULE", start, postdataFile)
	}
}

func PrintCreateTriggerStatements(postdataFile *utils.FileWithByteCount, toc *utils.TOC, triggers []QuerySimpleDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := postdataFile.ByteCount
		postdataFile.MustPrintf("\n\n%s;", trigger.Def)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(postdataFile, triggerMetadata[trigger.Oid], trigger.Name, "TRIGGER", tableFQN)
		toc.AddMetadataEntry(trigger.OwningSchema, trigger.Name, "TRIGGER", start, postdataFile)
	}
}

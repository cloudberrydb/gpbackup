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
		if index.TablespaceName != "" {
			postdataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", index.Name, index.TablespaceName)
		}
		PrintObjectMetadata(postdataFile, indexMetadata[index.Oid], index.Name, "INDEX")
		toc.AddPostdataEntry(index.OwningSchema, index.Name, "INDEX", start, postdataFile.ByteCount)
	}
}

func PrintCreateRuleStatements(postdataFile *utils.FileWithByteCount, toc *utils.TOC, rules []QuerySimpleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := postdataFile.ByteCount
		postdataFile.MustPrintf("\n\n%s", rule.Def)
		tableFQN := MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(postdataFile, ruleMetadata[rule.Oid], rule.Name, "RULE", tableFQN)
		toc.AddPostdataEntry(rule.OwningSchema, rule.Name, "RULE", start, postdataFile.ByteCount)
	}
}

func PrintCreateTriggerStatements(postdataFile *utils.FileWithByteCount, toc *utils.TOC, triggers []QuerySimpleDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := postdataFile.ByteCount
		postdataFile.MustPrintf("\n\n%s;", trigger.Def)
		tableFQN := MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(postdataFile, triggerMetadata[trigger.Oid], trigger.Name, "TRIGGER", tableFQN)
		toc.AddPostdataEntry(trigger.OwningSchema, trigger.Name, "TRIGGER", start, postdataFile.ByteCount)
	}
}

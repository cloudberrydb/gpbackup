package backup

/*
 * This file contains structs and functions related to dumping "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(postdataFile io.Writer, indexes []QuerySimpleDefinition, indexMetadata utils.MetadataMap) {
	for _, index := range indexes {
		utils.MustPrintf(postdataFile, "\n\n%s;", index.Def)
		PrintObjectMetadata(postdataFile, indexMetadata[index.Oid], index.Name, "INDEX")
	}
}

func PrintCreateRuleStatements(postdataFile io.Writer, rules []QuerySimpleDefinition, ruleMetadata utils.MetadataMap) {
	for _, rule := range rules {
		utils.MustPrintf(postdataFile, "\n\n%s", rule.Def)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(postdataFile, ruleMetadata[rule.Oid], rule.Name, "RULE", tableFQN)
	}
}

func PrintCreateTriggerStatements(postdataFile io.Writer, triggers []QuerySimpleDefinition, triggerMetadata utils.MetadataMap) {
	for _, trigger := range triggers {
		utils.MustPrintf(postdataFile, "\n\n%s;", trigger.Def)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(postdataFile, triggerMetadata[trigger.Oid], trigger.Name, "TRIGGER", tableFQN)
	}
}

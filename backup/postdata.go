package backup

/*
 * This file contains structs and functions related to dumping "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"fmt"
	"io"
	"sort"

	"github.com/greenplum-db/gpbackup/utils"
)

func GetIndexesForAllTables(connection *utils.DBConn, tables []utils.Relation) []string {
	indexes := make([]string, 0)
	for _, table := range tables {
		indexList := GetIndexMetadata(connection, table.RelationOid)
		for _, index := range indexList {
			indexStr := fmt.Sprintf("\n\n%s;", index.Def)
			if index.Comment != "" {
				indexStr += fmt.Sprintf("\nCOMMENT ON INDEX %s IS '%s';", utils.QuoteIdent(index.Name), index.Comment)
			}
			indexes = append(indexes, indexStr)
		}
	}
	return indexes
}

func GetRuleDefinitions(connection *utils.DBConn) []string {
	rules := make([]string, 0)
	ruleList := GetRuleMetadata(connection)
	for _, rule := range ruleList {
		ruleStr := fmt.Sprintf("\n\n%s", rule.Def)
		if rule.Comment != "" {
			ruleStr += fmt.Sprintf("\nCOMMENT ON RULE %s ON %s IS '%s';", utils.QuoteIdent(rule.Name), rule.OwningTable, rule.Comment)
		}
		rules = append(rules, ruleStr)
	}
	return rules
}

func GetTriggerDefinitions(connection *utils.DBConn) []string {
	triggers := make([]string, 0)
	triggerList := GetTriggerMetadata(connection)
	for _, trigger := range triggerList {
		triggerStr := fmt.Sprintf("\n\n%s;", trigger.Def)
		if trigger.Comment != "" {
			triggerStr += fmt.Sprintf("\nCOMMENT ON TRIGGER %s ON %s IS '%s';", utils.QuoteIdent(trigger.Name), trigger.OwningTable, trigger.Comment)
		}
		triggers = append(triggers, triggerStr)
	}
	return triggers
}

func PrintPostdataCreateStatements(postdataFile io.Writer, statements []string) {
	sort.Strings(statements)
	for _, statement := range statements {
		utils.MustPrintln(postdataFile, statement)
	}
}

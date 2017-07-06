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
			indexStr := fmt.Sprintf("\n\n%s;\n", index.Def)
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
			ruleStr += fmt.Sprintf("\nCOMMENT ON RULE %s IS '%s';", utils.QuoteIdent(rule.Name), rule.Comment)
		}
		rules = append(rules, ruleStr)
	}
	return rules
}

func PrintCreateIndexStatements(postdataFile io.Writer, indexes []string) {
	sort.Strings(indexes)
	for _, index := range indexes {
		utils.MustPrintln(postdataFile, index)
	}
}

func PrintCreateRuleStatements(postdataFile io.Writer, rules []string) {
	sort.Strings(rules)
	for _, rule := range rules {
		utils.MustPrintln(postdataFile, rule)
	}
}

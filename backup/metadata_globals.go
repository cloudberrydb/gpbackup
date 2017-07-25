package backup

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains structs and functions related to dumping global cluster
 * metadata on the master that needs to be restored before data is restored,
 * such as roles and database configuration.
 */

func PrintConnectionString(metadataFile io.Writer, dbname string) {
	utils.MustPrintf(metadataFile, "\\c %s\n", dbname)
}

/*
 * Session GUCs are printed to global, predata, and postdata files so we
 * will use the correct settings when the files are run during restore
 */
func PrintSessionGUCs(metadataFile io.Writer, gucs QuerySessionGUCs) {
	utils.MustPrintf(metadataFile, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = '%s';
SET standard_conforming_strings = %s;
SET default_with_oids = %s;
`, gucs.ClientEncoding, gucs.StdConformingStrings, gucs.DefaultWithOids)
}

func PrintCreateDatabaseStatement(globalFile io.Writer, dbname string, allDBs []QueryDatabaseName, dbMetadata MetadataMap) {
	dbname = utils.QuoteIdent(dbname)
	utils.MustPrintf(globalFile, "\n\nCREATE DATABASE %s;", dbname)
	for _, db := range allDBs {
		PrintObjectMetadata(globalFile, dbMetadata[db.Oid], db.Name, "DATABASE")
	}
}

func PrintDatabaseGUCs(globalFile io.Writer, gucs []string, dbname string) {
	for _, guc := range gucs {
		utils.MustPrintf(globalFile, "\nALTER DATABASE %s %s;", utils.QuoteIdent(dbname), guc)
	}
}

func PrintCreateResourceQueueStatements(globalFile io.Writer, resQueues []QueryResourceQueue, resQueueMetadata MetadataMap) {
	for _, resQueue := range resQueues {
		attributes := []string{}
		if resQueue.ActiveStatements != -1 {
			attributes = append(attributes, fmt.Sprintf("ACTIVE_STATEMENTS=%d", resQueue.ActiveStatements))
		}
		maxCostFloat, maxCostErr := strconv.ParseFloat(resQueue.MaxCost, 64)
		utils.CheckError(maxCostErr)
		if maxCostFloat > -1 {
			attributes = append(attributes, fmt.Sprintf("MAX_COST=%s", resQueue.MaxCost))
		}
		if resQueue.CostOvercommit {
			attributes = append(attributes, "COST_OVERCOMMIT=TRUE")
		}
		minCostFloat, minCostErr := strconv.ParseFloat(resQueue.MinCost, 64)
		utils.CheckError(minCostErr)
		if minCostFloat > 0 {
			attributes = append(attributes, fmt.Sprintf("MIN_COST=%s", resQueue.MinCost))
		}
		if resQueue.Priority != "medium" {
			attributes = append(attributes, fmt.Sprintf("PRIORITY=%s", strings.ToUpper(resQueue.Priority)))
		}
		if resQueue.MemoryLimit != "-1" {
			attributes = append(attributes, fmt.Sprintf("MEMORY_LIMIT='%s'", resQueue.MemoryLimit))
		}
		action := "CREATE"
		if resQueue.Name == "pg_default" {
			action = "ALTER"
		}
		utils.MustPrintf(globalFile, "\n\n%s RESOURCE QUEUE %s WITH (%s);", action, utils.QuoteIdent(resQueue.Name), strings.Join(attributes, ", "))
		PrintObjectMetadata(globalFile, resQueueMetadata[resQueue.Oid], utils.QuoteIdent(resQueue.Name), "RESOURCE QUEUE")
	}
}

func PrintCreateRoleStatements(globalFile io.Writer, roles []QueryRole, roleMetadata MetadataMap) {
	for _, role := range roles {
		quotedName := utils.QuoteIdent(role.Name)
		attrs := []string{}

		if role.Super {
			attrs = append(attrs, "SUPERUSER")
		} else {
			attrs = append(attrs, "NOSUPERUSER")
		}

		if role.Inherit {
			attrs = append(attrs, "INHERIT")
		} else {
			attrs = append(attrs, "NOINHERIT")
		}

		if role.CreateRole {
			attrs = append(attrs, "CREATEROLE")
		} else {
			attrs = append(attrs, "NOCREATEROLE")
		}

		if role.CreateDB {
			attrs = append(attrs, "CREATEDB")
		} else {
			attrs = append(attrs, "NOCREATEDB")
		}

		if role.CanLogin {
			attrs = append(attrs, "LOGIN")
		} else {
			attrs = append(attrs, "NOLOGIN")
		}
		if role.ConnectionLimit != -1 {
			attrs = append(attrs, fmt.Sprintf("CONNECTION LIMIT %d", role.ConnectionLimit))
		}

		if role.Password != "" {
			attrs = append(attrs, fmt.Sprintf("PASSWORD '%s'", role.Password))
		}

		if role.ValidUntil != "" {
			attrs = append(attrs, fmt.Sprintf("VALID UNTIL '%s'", role.ValidUntil))
		}

		attrs = append(attrs, fmt.Sprintf("RESOURCE QUEUE %s", utils.QuoteIdent(role.ResQueue)))

		if role.Createrexthttp {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='http')")
		}

		if role.Createrextgpfd {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gpfdist', type='readable')")
		}

		if role.Createwextgpfd {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gpfdist', type='writable')")
		}

		if role.Createrexthdfs {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gphdfs', type='readable')")
		}

		if role.Createwexthdfs {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gphdfs', type='writable')")
		}

		utils.MustPrintf(globalFile, `

CREATE ROLE %s;
ALTER ROLE %s WITH %s;`, quotedName, quotedName, strings.Join(attrs, " "))

		if len(role.TimeConstraints) != 0 {
			for _, timeConstraint := range role.TimeConstraints {
				utils.MustPrintf(globalFile, "\nALTER ROLE %s DENY BETWEEN DAY %d TIME '%s' AND DAY %d TIME '%s';", quotedName, timeConstraint.StartDay, timeConstraint.StartTime, timeConstraint.EndDay, timeConstraint.EndTime)
			}
		}
		PrintObjectMetadata(globalFile, roleMetadata[role.Oid], quotedName, "ROLE")
	}
}

func PrintRoleMembershipStatements(globalFile io.Writer, roleMembers []QueryRoleMember) {
	utils.MustPrintln(globalFile, "\n")
	for _, roleMember := range roleMembers {
		utils.MustPrintf(globalFile, "\nGRANT %s TO %s", roleMember.Role, roleMember.Member)
		if roleMember.IsAdmin {
			utils.MustPrintf(globalFile, " WITH ADMIN OPTION")
		}
		utils.MustPrintf(globalFile, " GRANTED BY %s;", roleMember.Grantor)
	}
}

func PrintCreateTablespaceStatements(globalFile io.Writer, tablespaces []QueryTablespace, tablespaceMetadata MetadataMap) {
	for _, tablespace := range tablespaces {
		utils.MustPrintf(globalFile, "\n\nCREATE TABLESPACE %s FILESPACE %s;", tablespace.Tablespace, tablespace.Filespace)
		PrintObjectMetadata(globalFile, tablespaceMetadata[tablespace.Oid], tablespace.Tablespace, "TABLESPACE")
	}
}

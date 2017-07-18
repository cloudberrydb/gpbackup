package backup

/*
 * This file contains structs and functions related to dumping metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * master that needs to be restored before data is restored.
 */

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

type ObjectMetadata struct {
	Privileges []ACL
	Owner      string
	Comment    string
}

type ACL struct {
	Grantee    string
	Select     bool
	Insert     bool
	Update     bool
	Delete     bool
	Truncate   bool
	References bool
	Trigger    bool
	Usage      bool
	Execute    bool
	Create     bool
	CreateTemp bool
	Connect    bool
}

type MetadataMap map[uint32]ObjectMetadata

func PrintObjectMetadata(file io.Writer, obj ObjectMetadata, objectName string, objectType string, owningTable ...string) {
	utils.MustPrintf(file, obj.GetCommentStatement(objectName, objectType, owningTable...))
	utils.MustPrintf(file, obj.GetOwnerStatement(objectName, objectType))
	utils.MustPrintf(file, obj.GetPrivilegesStatements(objectName, objectType))
}

func ParseACL(aclStr string) *ACL {
	aclRegex := regexp.MustCompile(`^(?:\"(.*)\"|(.*))=([a-zA-Z]*)/(?:\"(.*)\"|(.*))$`)
	grantee := ""
	acl := ACL{}
	if matches := aclRegex.FindStringSubmatch(aclStr); len(matches) != 0 {
		if matches[1] != "" {
			grantee = matches[1]
		} else if matches[2] != "" {
			grantee = matches[2]
		} else {
			grantee = "" // Empty string indicates privileges granted to PUBLIC
		}
		permStr := matches[3]
		for _, char := range permStr {
			switch char {
			case 'a':
				acl.Insert = true
			case 'r':
				acl.Select = true
			case 'w':
				acl.Update = true
			case 'd':
				acl.Delete = true
			case 'D':
				acl.Truncate = true
			case 'x':
				acl.References = true
			case 't':
				acl.Trigger = true
			case 'X':
				acl.Execute = true
			case 'U':
				acl.Usage = true
			case 'C':
				acl.Create = true
			case 'T':
				acl.CreateTemp = true
			case 'c':
				acl.Connect = true
			}
		}
		acl.Grantee = grantee
		return &acl
	}
	return nil
}

func (obj ObjectMetadata) GetPrivilegesStatements(objectName string, objectType string) string {
	statements := []string{}
	typeStr := fmt.Sprintf("%s ", objectType)
	if objectType == "VIEW" {
		typeStr = ""
	}
	if len(obj.Privileges) != 0 {
		statements = append(statements, fmt.Sprintf("REVOKE ALL ON %s%s FROM PUBLIC;", typeStr, objectName))
		if obj.Owner != "" {
			statements = append(statements, fmt.Sprintf("REVOKE ALL ON %s%s FROM %s;", typeStr, objectName, utils.QuoteIdent(obj.Owner)))
		}
		for _, acl := range obj.Privileges {
			/*
			 * Determine whether to print "GRANT ALL" instead of granting individual
			 * privileges.  Information on which privileges exist for a given object
			 * comes from src/include/utils/acl.h in GPDB.
			 */
			hasAllPrivileges := false
			grantStr := ""
			grantee := ""
			if acl.Grantee == "" {
				grantee = "PUBLIC"
			} else {
				grantee = utils.QuoteIdent(acl.Grantee)
			}
			switch objectType {
			case "DATABASE":
				hasAllPrivileges = acl.Create && acl.CreateTemp && acl.Connect
			case "FUNCTION":
				hasAllPrivileges = acl.Execute
			case "LANGUAGE":
				hasAllPrivileges = acl.Usage
			case "PROTOCOL":
				hasAllPrivileges = acl.Select && acl.Insert
			case "SCHEMA":
				hasAllPrivileges = acl.Usage && acl.Create
			case "SEQUENCE":
				hasAllPrivileges = acl.Select && acl.Update && acl.Usage
			case "TABLE":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
			case "VIEW":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
			}
			if hasAllPrivileges {
				grantStr = "ALL"
			} else {
				grantList := make([]string, 0)
				if acl.Select {
					grantList = append(grantList, "SELECT")
				}
				if acl.Insert {
					grantList = append(grantList, "INSERT")
				}
				if acl.Update {
					grantList = append(grantList, "UPDATE")
				}
				if acl.Delete {
					grantList = append(grantList, "DELETE")
				}
				if acl.Truncate {
					grantList = append(grantList, "TRUNCATE")
				}
				if acl.References {
					grantList = append(grantList, "REFERENCES")
				}
				if acl.Trigger {
					grantList = append(grantList, "TRIGGER")
				}
				if acl.Execute {
					grantList = append(grantList, "EXECUTE")
				}
				if acl.Usage {
					grantList = append(grantList, "USAGE")
				}
				grantStr = strings.Join(grantList, ",")
			}
			if grantStr != "" {
				statements = append(statements, fmt.Sprintf("GRANT %s ON %s%s TO %s;", grantStr, typeStr, objectName, grantee))
			}
		}
	}
	if len(statements) > 0 {
		return "\n\n" + strings.Join(statements, "\n")
	}
	return ""
}

func (obj ObjectMetadata) GetOwnerStatement(objectName string, objectType string) string {
	if objectType == "VIEW" {
		return ""
	}
	typeStr := objectType
	if objectType == "SEQUENCE" {
		typeStr = "TABLE"
	}
	ownerStr := ""
	if obj.Owner != "" {
		ownerStr = fmt.Sprintf("\n\nALTER %s %s OWNER TO %s;\n", typeStr, objectName, utils.QuoteIdent(obj.Owner))
	}
	return ownerStr
}

func (obj ObjectMetadata) GetCommentStatement(objectName string, objectType string, owningTable ...string) string {
	commentStr := ""
	tableStr := ""
	if len(owningTable) == 1 {
		tableStr = fmt.Sprintf(" ON %s", owningTable[0])
	}
	if obj.Comment != "" {
		commentStr = fmt.Sprintf("\n\nCOMMENT ON %s %s%s IS '%s';\n", objectType, objectName, tableStr, obj.Comment)
	}
	return commentStr
}

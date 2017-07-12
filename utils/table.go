package utils

/*
 * This file contains structs and functions related to storing schema and
 * table information for querying the database and filtering backup lists.
 */

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

var (
	/* To be used in a Postgres query without being quoted, an identifier (schema or
	 * table) must begin with a lowercase letter or underscore, and may contain only
	 * lowercast letters, digits, and underscores.
	 */
	unquotedIdentifier = regexp.MustCompile(`^([a-z_][a-z0-9_]*)$`)
	quotedIdentifier   = regexp.MustCompile(`^"(.+)"$`)

	quotedOrUnquotedString = regexp.MustCompile(`^(?:\"(.*)\"|(.*))\.(?:\"(.*)\"|(.*))$`)

	// Swap between double quotes and paired double quotes, and between literal whitespace characters and escape sequences
	replacerEscape   = strings.NewReplacer(`"`, `""`, `\`, `\\`)
	replacerUnescape = strings.NewReplacer(`""`, `"`, `\\`, `\`)
)

// This will mostly be used for schemas, but can be used for any database object with an oid.
type Schema struct {
	Oid  uint32
	Name string
}

type Relation struct {
	SchemaOid    uint32
	RelationOid  uint32
	SchemaName   string
	RelationName string
}

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

/*
 * Functions for escaping schemas and tables
 */

// This function quotes an unquoted identifier like quote_ident() in Postgres.
func QuoteIdent(ident string) string {
	if !unquotedIdentifier.MatchString(ident) {
		ident = replacerEscape.Replace(ident)
		ident = fmt.Sprintf(`"%s"`, ident)
	}
	return ident
}

func MakeFQN(schema string, object string) string {
	schema = QuoteIdent(schema)
	object = QuoteIdent(object)
	return fmt.Sprintf("%s.%s", schema, object)
}

/*
 * The following functions create structs with reasonable default values set,
 * for use in RelationFromString and to make creating sample structs in tests
 * easier.
 */

func BasicRelation(schema string, relation string) Relation {
	return Relation{
		SchemaOid:    0,
		SchemaName:   schema,
		RelationOid:  0,
		RelationName: relation,
	}
}

func DefaultACLForType(grantee string, objType string) ACL {
	return ACL{
		Grantee:    grantee,
		Select:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		Insert:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW",
		Update:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		Delete:     objType == "TABLE" || objType == "VIEW",
		Truncate:   objType == "TABLE" || objType == "VIEW",
		References: objType == "TABLE" || objType == "VIEW",
		Trigger:    objType == "TABLE" || objType == "VIEW",
		Usage:      objType == "LANGUAGE" || objType == "SCHEMA" || objType == "SEQUENCE",
		Execute:    objType == "FUNCTION",
		Create:     objType == "DATABASE" || objType == "SCHEMA",
		CreateTemp: objType == "DATABASE",
		Connect:    objType == "DATABASE",
	}
}

func DefaultACLWithout(grantee string, objType string, revoke ...string) ACL {
	defaultACL := DefaultACLForType(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.Select = false
		case "INSERT":
			defaultACL.Insert = false
		case "UPDATE":
			defaultACL.Update = false
		case "DELETE":
			defaultACL.Delete = false
		case "TRUNCATE":
			defaultACL.Truncate = false
		case "REFERENCES":
			defaultACL.References = false
		case "TRIGGER":
			defaultACL.Trigger = false
		case "EXECUTE":
			defaultACL.Execute = false
		case "USAGE":
			defaultACL.Usage = false
		}
	}
	return defaultACL
}

/*
 * Functions for struct input/parsing and output
 */

/*
 * This function prints a table in fully-qualified schema.table format, with
 * everything quoted and escaped appropriately.
 */
func (t Relation) ToString() string {
	return MakeFQN(t.SchemaName, t.RelationName)
}

func (s Schema) ToString() string {
	return QuoteIdent(s.Name)
}

/* Parse an appropriately-escaped schema.table string into a Relation.  The Relation's
 * Oid fields are left at 0, and will need to be filled in with the real values
 * if the Relation is to be used in any Get[Something]() function in queries.go.
 */
func RelationFromString(name string) Relation {
	var schema, table string
	var matches []string
	if matches = quotedOrUnquotedString.FindStringSubmatch(name); len(matches) != 0 {
		if matches[1] != "" { // schema was quoted
			schema = replacerUnescape.Replace(matches[1])
		} else { // schema wasn't quoted
			schema = replacerUnescape.Replace(matches[2])
		}
		if matches[3] != "" { // table was quoted
			table = replacerUnescape.Replace(matches[3])
		} else { // table wasn't quoted
			table = replacerUnescape.Replace(matches[4])
		}
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid fully-qualified table expression", name), "")
	}
	return BasicRelation(schema, table)
}

func SchemaFromString(name string) Schema {
	var schema string
	var matches []string
	if matches = quotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = replacerUnescape.Replace(matches[1])
	} else if matches = unquotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = replacerUnescape.Replace(matches[1])
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid identifier", name), "")
	}
	return Schema{0, schema}
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
			statements = append(statements, fmt.Sprintf("REVOKE ALL ON %s%s FROM %s;", typeStr, objectName, QuoteIdent(obj.Owner)))
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
				grantee = QuoteIdent(acl.Grantee)
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
		ownerStr = fmt.Sprintf("\n\nALTER %s %s OWNER TO %s;\n", typeStr, objectName, QuoteIdent(obj.Owner))
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

/*
 * Functions for sorting schemas and tables
 */

type Schemas []Schema

func (slice Schemas) Len() int {
	return len(slice)
}

func (slice Schemas) Less(i int, j int) bool {
	return slice[i].Name < slice[j].Name
}

func (slice Schemas) Swap(i int, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func SortSchemas(objects Schemas) {
	sort.Sort(objects)
}

type Relations []Relation

func (slice Relations) Len() int {
	return len(slice)
}

func (slice Relations) Less(i int, j int) bool {
	if slice[i].SchemaName < slice[j].SchemaName {
		return true
	}
	if slice[i].RelationName < slice[j].RelationName {
		return true
	}
	return false
}

func (slice Relations) Swap(i int, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func SortRelations(tables Relations) {
	sort.Sort(tables)
}

/*
 * Other schema- and table-related utility functions
 */

/*
 * Given a list of Relations, this function returns a sorted list of their Schemas.
 * It assumes that the Relation list is sorted by schema and then by table, so it
 * doesn't need to do any sorting itself.
 */
func GetUniqueSchemas(schemas []Schema, tables []Relation) []Schema {
	currentSchemaOid := uint32(0)
	uniqueSchemas := make([]Schema, 0)
	schemaMap := make(map[uint32]Schema, 0)
	for _, schema := range schemas {
		schemaMap[schema.Oid] = schema
	}
	for _, table := range tables {
		if table.SchemaOid != currentSchemaOid {
			currentSchemaOid = table.SchemaOid
			uniqueSchemas = append(uniqueSchemas, schemaMap[currentSchemaOid])
		}
	}
	return uniqueSchemas
}

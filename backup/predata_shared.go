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

func MakeFQN(schema string, object string) string {
	schema = utils.QuoteIdent(schema)
	object = utils.QuoteIdent(object)
	return fmt.Sprintf("%s.%s", schema, object)
}

/*
 * Structs and functions relating to generic metadata handling.
 */

type ObjectMetadata struct {
	Privileges []ACL
	Owner      string
	Comment    string
}

type ACL struct {
	Grantee             string
	Select              bool
	SelectWithGrant     bool
	Insert              bool
	InsertWithGrant     bool
	Update              bool
	UpdateWithGrant     bool
	Delete              bool
	DeleteWithGrant     bool
	Truncate            bool
	TruncateWithGrant   bool
	References          bool
	ReferencesWithGrant bool
	Trigger             bool
	TriggerWithGrant    bool
	Usage               bool
	UsageWithGrant      bool
	Execute             bool
	ExecuteWithGrant    bool
	Create              bool
	CreateWithGrant     bool
	Temporary           bool
	TemporaryWithGrant  bool
	Connect             bool
	ConnectWithGrant    bool
}

type MetadataMap map[uint32]ObjectMetadata

func PrintObjectMetadata(file io.Writer, obj ObjectMetadata, objectName string, objectType string, owningTable ...string) {
	if comment := obj.GetCommentStatement(objectName, objectType, owningTable...); comment != "" {
		utils.MustPrintln(file, comment)
	}
	if owner := obj.GetOwnerStatement(objectName, objectType); owner != "" {
		utils.MustPrintln(file, owner)
	}
	if privileges := obj.GetPrivilegesStatements(objectName, objectType); privileges != "" {
		utils.MustPrintln(file, privileges)
	}
}

func ParseACL(aclStr string) *ACL {
	aclRegex := regexp.MustCompile(`^(?:\"(.*)\"|(.*))=([a-zA-Z\*]*)/(?:\"(.*)\"|(.*))$`)
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
		var lastChar rune
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
				acl.Temporary = true
			case 'c':
				acl.Connect = true
			case '*':
				switch lastChar {
				case 'a':
					acl.Insert = false
					acl.InsertWithGrant = true
				case 'r':
					acl.Select = false
					acl.SelectWithGrant = true
				case 'w':
					acl.Update = false
					acl.UpdateWithGrant = true
				case 'd':
					acl.Delete = false
					acl.DeleteWithGrant = true
				case 'D':
					acl.Truncate = false
					acl.TruncateWithGrant = true
				case 'x':
					acl.References = false
					acl.ReferencesWithGrant = true
				case 't':
					acl.Trigger = false
					acl.TriggerWithGrant = true
				case 'X':
					acl.Execute = false
					acl.ExecuteWithGrant = true
				case 'U':
					acl.Usage = false
					acl.UsageWithGrant = true
				case 'C':
					acl.Create = false
					acl.CreateWithGrant = true
				case 'T':
					acl.Temporary = false
					acl.TemporaryWithGrant = true
				case 'c':
					acl.Connect = false
					acl.ConnectWithGrant = true
				}
			}
			lastChar = char
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
			hasAllPrivilegesWithGrant := false
			privStr := ""
			privWithGrantStr := ""
			grantee := ""
			if acl.Grantee == "" {
				grantee = "PUBLIC"
			} else {
				grantee = utils.QuoteIdent(acl.Grantee)
			}
			switch objectType {
			case "DATABASE":
				hasAllPrivileges = acl.Create && acl.Temporary && acl.Connect
				hasAllPrivilegesWithGrant = acl.CreateWithGrant && acl.TemporaryWithGrant && acl.ConnectWithGrant
			case "FUNCTION":
				hasAllPrivileges = acl.Execute
				hasAllPrivilegesWithGrant = acl.ExecuteWithGrant
			case "LANGUAGE":
				hasAllPrivileges = acl.Usage
				hasAllPrivilegesWithGrant = acl.UsageWithGrant
			case "PROTOCOL":
				hasAllPrivileges = acl.Select && acl.Insert
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant
			case "SCHEMA":
				hasAllPrivileges = acl.Usage && acl.Create
				hasAllPrivilegesWithGrant = acl.UsageWithGrant && acl.CreateWithGrant
			case "SEQUENCE":
				hasAllPrivileges = acl.Select && acl.Update && acl.Usage
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.UpdateWithGrant && acl.UsageWithGrant
			case "TABLE":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
					acl.TruncateWithGrant && acl.ReferencesWithGrant && acl.TriggerWithGrant
			case "TABLESPACE":
				hasAllPrivileges = acl.Create
				hasAllPrivilegesWithGrant = acl.CreateWithGrant
			case "VIEW":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
					acl.TruncateWithGrant && acl.ReferencesWithGrant && acl.TriggerWithGrant
			}
			if hasAllPrivileges {
				privStr = "ALL"
			} else {
				privList := make([]string, 0)
				if acl.Select {
					privList = append(privList, "SELECT")
				}
				if acl.Insert {
					privList = append(privList, "INSERT")
				}
				if acl.Update {
					privList = append(privList, "UPDATE")
				}
				if acl.Delete {
					privList = append(privList, "DELETE")
				}
				if acl.Truncate {
					privList = append(privList, "TRUNCATE")
				}
				if acl.References {
					privList = append(privList, "REFERENCES")
				}
				if acl.Trigger {
					privList = append(privList, "TRIGGER")
				}
				if acl.Execute {
					privList = append(privList, "EXECUTE")
				}
				if acl.Usage {
					privList = append(privList, "USAGE")
				}
				if acl.Create {
					privList = append(privList, "CREATE")
				}
				if acl.Temporary {
					privList = append(privList, "TEMPORARY")
				}
				if acl.Connect {
					privList = append(privList, "CONNECT")
				}
				privStr = strings.Join(privList, ",")
			}
			if hasAllPrivilegesWithGrant {
				privWithGrantStr = "ALL"
			} else {
				privWithGrantList := make([]string, 0)
				if acl.SelectWithGrant {
					privWithGrantList = append(privWithGrantList, "SELECT")
				}
				if acl.InsertWithGrant {
					privWithGrantList = append(privWithGrantList, "INSERT")
				}
				if acl.UpdateWithGrant {
					privWithGrantList = append(privWithGrantList, "UPDATE")
				}
				if acl.DeleteWithGrant {
					privWithGrantList = append(privWithGrantList, "DELETE")
				}
				if acl.TruncateWithGrant {
					privWithGrantList = append(privWithGrantList, "TRUNCATE")
				}
				if acl.ReferencesWithGrant {
					privWithGrantList = append(privWithGrantList, "REFERENCES")
				}
				if acl.TriggerWithGrant {
					privWithGrantList = append(privWithGrantList, "TRIGGER")
				}
				if acl.ExecuteWithGrant {
					privWithGrantList = append(privWithGrantList, "EXECUTE")
				}
				if acl.UsageWithGrant {
					privWithGrantList = append(privWithGrantList, "USAGE")
				}
				if acl.CreateWithGrant {
					privWithGrantList = append(privWithGrantList, "CREATE")
				}
				if acl.TemporaryWithGrant {
					privWithGrantList = append(privWithGrantList, "TEMPORARY")
				}
				if acl.ConnectWithGrant {
					privWithGrantList = append(privWithGrantList, "CONNECT")
				}
				privWithGrantStr = strings.Join(privWithGrantList, ",")
			}
			if privStr != "" {
				statements = append(statements, fmt.Sprintf("GRANT %s ON %s%s TO %s;", privStr, typeStr, objectName, grantee))
			}
			if privWithGrantStr != "" {
				statements = append(statements, fmt.Sprintf("GRANT %s ON %s%s TO %s WITH GRANT OPTION;", privWithGrantStr, typeStr, objectName, grantee))
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
		ownerStr = fmt.Sprintf("\n\nALTER %s %s OWNER TO %s;", typeStr, objectName, utils.QuoteIdent(obj.Owner))
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
		commentStr = fmt.Sprintf("\n\nCOMMENT ON %s %s%s IS '%s';", objectType, objectName, tableStr, obj.Comment)
	}
	return commentStr
}

func SortFunctionsAndTypesInDependencyOrder(types []TypeDefinition, functions []QueryFunctionDefinition) []Sortable {
	objects := make([]Sortable, 0)
	for _, typ := range types {
		if typ.Type != "e" && typ.Type != "p" {
			objects = append(objects, typ)
		}
	}
	for _, function := range functions {
		objects = append(objects, function)
	}
	sorted := TopologicalSort(objects)
	return sorted
}

func ConstructDependencyLists(connection *utils.DBConn, types []TypeDefinition, functions []QueryFunctionDefinition) ([]TypeDefinition, []QueryFunctionDefinition) {
	types = CoalesceCompositeTypes(types)
	types = ConstructBaseTypeDependencies(connection, types)
	types = ConstructDomainDependencies(connection, types)
	types = ConstructCompositeTypeDependencies(connection, types)
	functions = ConstructFunctionDependencies(connection, functions)
	return types, functions
}

func ConstructFunctionAndTypeMetadataMap(types MetadataMap, functions MetadataMap) MetadataMap {
	metadataMap := make(MetadataMap, 0)
	for k, v := range types {
		metadataMap[k] = v
	}
	for k, v := range functions {
		metadataMap[k] = v
	}
	return metadataMap
}

func PrintCreateDependentTypeAndFunctionStatements(predataFile io.Writer, objects []Sortable, metadataMap MetadataMap) {
	for _, object := range objects {
		switch obj := object.(type) {
		case TypeDefinition:
			switch obj.Type {
			case "b":
				PrintCreateBaseTypeStatement(predataFile, obj, metadataMap[obj.Oid])
			case "c":
				PrintCreateCompositeTypeStatement(predataFile, obj, metadataMap[obj.Oid])
			case "d":
				PrintCreateDomainStatement(predataFile, obj, metadataMap[obj.Oid])
			}
		case QueryFunctionDefinition:
			PrintCreateFunctionStatement(predataFile, obj, metadataMap[obj.Oid])
		}
	}
}

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	Name() string
	Dependencies() []string
}

func (r Relation) Name() string {
	return r.ToString()
}

func (v QueryViewDefinition) Name() string {
	return MakeFQN(v.SchemaName, v.ViewName)
}

func (f QueryFunctionDefinition) Name() string {
	return MakeFQN(f.SchemaName, f.FunctionName)
}

func (t TypeDefinition) Name() string {
	return MakeFQN(t.TypeSchema, t.TypeName)
}

func (r Relation) Dependencies() []string {
	return r.DependsUpon
}

func (v QueryViewDefinition) Dependencies() []string {
	return v.DependsUpon
}

func (f QueryFunctionDefinition) Dependencies() []string {
	return f.DependsUpon
}

func (t TypeDefinition) Dependencies() []string {
	return t.DependsUpon
}

func SortRelations(relations []Relation) []Relation {
	sortable := make([]Sortable, len(relations))
	for i := range relations {
		sortable[i] = relations[i]
	}
	sortable = TopologicalSort(sortable)
	for i := range sortable {
		relations[i] = sortable[i].(Relation)
	}
	return relations
}

func SortViews(views []QueryViewDefinition) []QueryViewDefinition {
	sortable := make([]Sortable, len(views))
	for i := range views {
		sortable[i] = views[i]
	}
	sortable = TopologicalSort(sortable)
	for i := range views {
		views[i] = sortable[i].(QueryViewDefinition)
	}
	return views
}

func TopologicalSort(slice []Sortable) []Sortable {
	inDegrees := make(map[string]int, 0)
	dependencyIndexes := make(map[string]int, 0)
	isDependentOn := make(map[string][]string, 0)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[string]bool)
	for i, item := range slice {
		name := item.Name()
		deps := item.Dependencies()
		notVisited[name] = true
		inDegrees[name] = len(deps)
		for _, dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], name)
		}
		dependencyIndexes[name] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sorted = append(sorted, item)
		delete(notVisited, item.Name())
		for _, dep := range isDependentOn[item.Name()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				queue = append(queue, slice[dependencyIndexes[dep]])
			}
		}
	}
	return sorted
}

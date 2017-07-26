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
	DependsUpon  []string
}

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

/*
 * Dependencies are sorted before tables that depend on them.  We return false
 * for all comparisons and true by default to ensure that the entire list is
 * traversed.
 */
func (slice Relations) Less(i int, j int) bool {
	for _, dependencyFQN := range slice[i].DependsUpon {
		if slice[j].ToString() == dependencyFQN {
			return false
		}
	}
	return true
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

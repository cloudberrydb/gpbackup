package utils

/*
 * This file contains structs and functions related to storing schema and
 * table information for querying the database and filtering backup lists.
 */

import (
	"database/sql"
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
	replacerTo = strings.NewReplacer("\"", "\"\"", `
`, "\\n", "\n", "\\n", "	", "\\t", "\t", "\\t")
	replacerFrom = strings.NewReplacer("\"\"", "\"", "\\n", "\n", "\\t", "\t")
)

// This will mostly be used for schemas, but can be used for any database object with an oid.
type Schema struct {
	SchemaOid  uint32
	SchemaName string
	Comment    sql.NullString
}

type Relation struct {
	SchemaOid    uint32
	RelationOid  uint32
	SchemaName   string
	RelationName string
	Comment      sql.NullString
}

/*
 * Functions for escaping schemas and tables
 */

// This function quotes an unquoted identifier like quote_ident() in Postgres.
func QuoteIdent(ident string) string {
	if !unquotedIdentifier.MatchString(ident) {
		ident = replacerTo.Replace(ident)
		ident = fmt.Sprintf(`"%s"`, ident)
	}
	return ident
}

/*
 * This function prints a table in fully-qualified schema.table format, with
 * everything quoted and escaped appropriately.
 */
func (t Relation) ToString() string {
	schema := QuoteIdent(t.SchemaName)
	table := QuoteIdent(t.RelationName)
	return fmt.Sprintf("%s.%s", schema, table)
}

func (s Schema) ToString() string {
	return QuoteIdent(s.SchemaName)
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
			schema = replacerFrom.Replace(matches[1])
		} else { // schema wasn't quoted
			schema = replacerFrom.Replace(matches[2])
		}
		if matches[3] != "" { // table was quoted
			table = replacerFrom.Replace(matches[3])
		} else { // table wasn't quoted
			table = replacerFrom.Replace(matches[4])
		}
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid fully-qualified table expression", name), "")
	}
	return Relation{0, 0, schema, table, sql.NullString{"", false}}
}

func SchemaFromString(name string) Schema {
	var object string
	var matches []string
	if matches = quotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		object = replacerFrom.Replace(matches[1])
	} else if matches = unquotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		object = replacerFrom.Replace(matches[1])
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid identifier", name), "")
	}
	return Schema{0, object, sql.NullString{"", false}}
}

/*
 * Functions for sorting schemas and tables
 */

type Schemas []Schema

func (slice Schemas) Len() int {
	return len(slice)
}

func (slice Schemas) Less(i int, j int) bool {
	return slice[i].SchemaName < slice[j].SchemaName
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
		schemaMap[schema.SchemaOid] = schema
	}
	for _, table := range tables {
		if table.SchemaOid != currentSchemaOid {
			currentSchemaOid = table.SchemaOid
			uniqueSchemas = append(uniqueSchemas, schemaMap[currentSchemaOid])
		}
	}
	return uniqueSchemas
}

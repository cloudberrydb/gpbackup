package backup

/*
 * This file contains structs and functions related to dumping relation
 * (sequence, table, and view) metadata on the master.
 */

import (
	"fmt"
	"io"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

type Relation struct {
	SchemaOid    uint32
	RelationOid  uint32
	SchemaName   string
	RelationName string
	DependsUpon  []string // Used for dependency sorting
	Inherits     []string // Only used for printing INHERITS statement
}

/*
 * This function prints a table in fully-qualified schema.table format, with
 * everything quoted and escaped appropriately.
 */
func (t Relation) ToString() string {
	return MakeFQN(t.SchemaName, t.RelationName)
}

/* Parse an appropriately-escaped schema.table string into a Relation.  The Relation's
 * Oid fields are left at 0, and will need to be filled in with the real values
 * if the Relation is to be used in any Get[Something]() function in queries.go.
 */
func RelationFromString(name string) Relation {
	var schema, table string
	var matches []string
	if matches = utils.QuotedOrUnquotedString.FindStringSubmatch(name); len(matches) != 0 {
		if matches[1] != "" { // schema was quoted
			schema = utils.ReplacerUnescape.Replace(matches[1])
		} else { // schema wasn't quoted
			schema = utils.ReplacerUnescape.Replace(matches[2])
		}
		if matches[3] != "" { // table was quoted
			table = utils.ReplacerUnescape.Replace(matches[3])
		} else { // table wasn't quoted
			table = utils.ReplacerUnescape.Replace(matches[4])
		}
	} else {
		logger.Fatal(errors.Errorf("%s is not a valid fully-qualified table expression", name), "")
	}
	return BasicRelation(schema, table)
}

func BasicRelation(schema string, relation string) Relation {
	return Relation{
		SchemaOid:    0,
		SchemaName:   schema,
		RelationOid:  0,
		RelationName: relation,
	}
}

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

type TableDefinition struct {
	DistPolicy      string
	PartDef         string
	PartTemplateDef string
	StorageOpts     string
	TablespaceName  string
	ColumnDefs      []ColumnDefinition
	IsExternal      bool
	ExtTableDef     ExternalTableDefinition
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTables(connection *utils.DBConn, tables []Relation) map[uint32]TableDefinition {
	tableDefinitionMap := make(map[uint32]TableDefinition, 0)

	columnDefs := GetColumnDefinitions(connection)
	distributionPolicies := GetDistributionPolicies(connection, tables)
	partitionDefs := GetPartitionDefinitions(connection)
	partTemplateDefs := GetPartitionTemplates(connection)
	storageOptions := GetStorageOptions(connection)
	tablespaceNames := GetTablespaceNames(connection)
	extTableDefs := GetExternalTableDefinitions(connection)

	for _, table := range tables {
		oid := table.RelationOid
		tableDefinitionMap[oid] = TableDefinition{
			distributionPolicies[oid],
			partitionDefs[oid],
			partTemplateDefs[oid],
			storageOptions[oid],
			tablespaceNames[oid],
			columnDefs[oid],
			(extTableDefs[oid].Oid != 0),
			extTableDefs[oid],
		}
	}
	return tableDefinitionMap
}

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(predataFile io.Writer, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	if tableDef.IsExternal {
		PrintExternalTableCreateStatement(predataFile, table, tableDef)
	} else {
		PrintRegularTableCreateStatement(predataFile, table, tableDef)
	}
	PrintPostCreateTableStatements(predataFile, table, tableDef, tableMetadata)
}

func PrintRegularTableCreateStatement(predataFile io.Writer, table Relation, tableDef TableDefinition) {
	utils.MustPrintf(predataFile, "\n\nCREATE TABLE %s (\n", table.ToString())
	printColumnDefinitions(predataFile, table, tableDef.ColumnDefs)
	utils.MustPrintf(predataFile, ") ")
	if len(table.Inherits) != 0 {
		dependencyList := strings.Join(table.Inherits, ", ")
		utils.MustPrintf(predataFile, "INHERITS (%s) ", dependencyList)
	}
	if tableDef.StorageOpts != "" {
		utils.MustPrintf(predataFile, "WITH (%s) ", tableDef.StorageOpts)
	}
	if tableDef.TablespaceName != "" {
		utils.MustPrintf(predataFile, "TABLESPACE %s ", tableDef.TablespaceName)
	}
	utils.MustPrintf(predataFile, "%s", tableDef.DistPolicy)
	if tableDef.PartDef != "" {
		utils.MustPrintf(predataFile, " %s", strings.TrimSpace(tableDef.PartDef))
	}
	utils.MustPrintln(predataFile, ";")
	if tableDef.PartTemplateDef != "" {
		utils.MustPrintf(predataFile, "%s;\n", strings.TrimSpace(tableDef.PartTemplateDef))
	}
	printAlterColumnStatements(predataFile, table, tableDef.ColumnDefs)
}

func printColumnDefinitions(predataFile io.Writer, table Relation, columnDefs []ColumnDefinition) {
	lines := make([]string, 0)
	for _, column := range columnDefs {
		if !column.IsDropped {
			line := fmt.Sprintf("\t%s %s", utils.QuoteIdent(column.Name), column.TypeName)
			if column.HasDefault {
				line += fmt.Sprintf(" DEFAULT %s", column.DefaultVal)
			}
			if column.NotNull {
				line += " NOT NULL"
			}
			if column.Encoding != "" {
				line += fmt.Sprintf(" ENCODING (%s)", column.Encoding)
			}
			lines = append(lines, line)
		}
	}
	if len(lines) > 0 {
		utils.MustPrintln(predataFile, strings.Join(lines, ",\n"))
	}
}

func printAlterColumnStatements(predataFile io.Writer, table Relation, columnDefs []ColumnDefinition) {
	for _, column := range columnDefs {
		if column.StatTarget > -1 {
			utils.MustPrintf(predataFile, "\nALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;", table.ToString(), column.Name, column.StatTarget)
		}
		if column.StorageType != "" {
			utils.MustPrintf(predataFile, "\nALTER TABLE ONLY %s ALTER COLUMN %s SET STORAGE %s;", table.ToString(), column.Name, column.StorageType)
		}
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(predataFile io.Writer, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	PrintObjectMetadata(predataFile, tableMetadata, table.ToString(), "TABLE")

	for _, att := range tableDef.ColumnDefs {
		if att.Comment != "" {
			utils.MustPrintf(predataFile, "\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.ToString(), utils.QuoteIdent(att.Name), att.Comment)
		}
	}
}

type Sequence struct {
	Relation
	SequenceDefinition
}

func GetAllSequences(connection *utils.DBConn) []Sequence {
	sequenceRelations := GetAllSequenceRelations(connection)
	sequences := make([]Sequence, 0)
	for _, seqRelation := range sequenceRelations {
		seqDef := GetSequenceDefinition(connection, seqRelation.ToString())
		sequence := Sequence{seqRelation, seqDef}
		sequences = append(sequences, sequence)
	}
	return sequences
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(predataFile io.Writer, sequences []Sequence, sequenceMetadata MetadataMap) {
	maxVal := int64(9223372036854775807)
	minVal := int64(-9223372036854775807)
	for _, sequence := range sequences {
		seqFQN := sequence.ToString()
		utils.MustPrintln(predataFile, "\n\nCREATE SEQUENCE", seqFQN)
		if !sequence.IsCalled {
			utils.MustPrintln(predataFile, "\tSTART WITH", sequence.LastVal)
		}
		utils.MustPrintln(predataFile, "\tINCREMENT BY", sequence.Increment)

		if !((sequence.MaxVal == maxVal && sequence.Increment > 0) || (sequence.MaxVal == -1 && sequence.Increment < 0)) {
			utils.MustPrintln(predataFile, "\tMAXVALUE", sequence.MaxVal)
		} else {
			utils.MustPrintln(predataFile, "\tNO MAXVALUE")
		}
		if !((sequence.MinVal == minVal && sequence.Increment < 0) || (sequence.MinVal == 1 && sequence.Increment > 0)) {
			utils.MustPrintln(predataFile, "\tMINVALUE", sequence.MinVal)
		} else {
			utils.MustPrintln(predataFile, "\tNO MINVALUE")
		}
		cycleStr := ""
		if sequence.IsCycled {
			cycleStr = "\n\tCYCLE"
		}
		utils.MustPrintf(predataFile, "\tCACHE %d%s;", sequence.CacheVal, cycleStr)

		utils.MustPrintf(predataFile, "\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", seqFQN, sequence.LastVal, sequence.IsCalled)

		PrintObjectMetadata(predataFile, sequenceMetadata[sequence.RelationOid], seqFQN, "SEQUENCE")
	}
}

func PrintAlterSequenceStatements(predataFile io.Writer, sequences []Sequence, sequenceColumnOwners map[string]string) {
	for _, sequence := range sequences {
		seqFQN := sequence.ToString()
		// owningColumn is quoted when the map is constructed in GetSequenceColumnOwnerMap() and doesn't need to be quoted again
		if owningColumn, hasColumnOwner := sequenceColumnOwners[seqFQN]; hasColumnOwner {
			utils.MustPrintf(predataFile, "\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, owningColumn)
		}
	}
}

func PrintCreateViewStatements(predataFile io.Writer, views []View, viewMetadata MetadataMap) {
	for _, view := range views {
		viewFQN := MakeFQN(view.SchemaName, view.ViewName)
		utils.MustPrintf(predataFile, "\n\nCREATE VIEW %s AS %s\n", viewFQN, view.Definition)
		PrintObjectMetadata(predataFile, viewMetadata[view.Oid], viewFQN, "VIEW")
	}
}

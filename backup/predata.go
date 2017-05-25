package backup

/*
 * This file contains structs and functions related to dumping "pre-data" metadata
 * on the master, which is any metadata that needs to be restored before data is
 * restored, such as table definitions and check constraints.
 */

import (
	"fmt"
	"gpbackup/utils"
	"io"
	"sort"
	"strings"
)

type ColumnDefinition struct {
	Num        int
	Name       string
	NotNull    bool
	HasDefault bool
	IsDropped  bool
	TypName    string
	Encoding   string
	Comment    string
	DefaultVal string
}

type ExternalTableDefinition struct {
	Type            int
	Protocol        int
	Location        string
	ExecLocation    string
	FormatType      string
	FormatOpts      string
	Options         string
	Command         string
	RejectLimit     int
	RejectLimitType string
	ErrTable        string
	Encoding        string
	Writable        bool
}

type TableDefinition struct {
	DistPolicy      string
	PartDef         string
	PartTemplateDef string
	StorageOpts     string
	ColumnDefs      []ColumnDefinition
	IsExternal      bool
	ExtTableDef     ExternalTableDefinition
}

type SequenceDefinition struct {
	utils.Relation
	QuerySequence
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTable(connection *utils.DBConn, table utils.Relation, isExternal bool) TableDefinition {
	tableAttributes := GetTableAttributes(connection, table.RelationOid)
	tableDefaults := GetTableDefaults(connection, table.RelationOid)

	distributionPolicy := GetDistributionPolicy(connection, table.RelationOid)
	partitionDef := GetPartitionDefinition(connection, table.RelationOid)
	partTemplateDef := GetPartitionTemplateDefinition(connection, table.RelationOid)
	storageOptions := GetStorageOptions(connection, table.RelationOid)

	columnDefs := ConsolidateColumnInfo(tableAttributes, tableDefaults)
	var extTableDef ExternalTableDefinition
	if isExternal {
		extTableDef = GetExternalTableDefinition(connection, table.RelationOid)
	}
	tableDef := TableDefinition{distributionPolicy, partitionDef, partTemplateDef, storageOptions, columnDefs, isExternal, extTableDef}
	return tableDef
}

/*
 * This function calls per-table functions to get constraints related to each
 * table, then consolidates them in two slices holding all constraints for all
 * tables.  Two slices are needed because FOREIGN KEY constraints must be dumped
 * after PRIMARY KEY constraints, so they're separated out to be handled last.
 */
func ConstructConstraintsForAllTables(connection *utils.DBConn, tables []utils.Relation) ([]string, []string) {
	allConstraints := make([]string, 0)
	allFkConstraints := make([]string, 0)
	for _, table := range tables {
		constraintList := GetConstraints(connection, table.RelationOid)
		tableConstraints, tableFkConstraints := ProcessConstraints(table, constraintList)
		allConstraints = append(allConstraints, tableConstraints...)
		allFkConstraints = append(allFkConstraints, tableFkConstraints...)
	}
	return allConstraints, allFkConstraints
}

/*
 * This function zips up table attributes and column default information into
 * one struct, instead of performing an expensive join to get everything in
 * a single query.
 */
func ConsolidateColumnInfo(atts []QueryTableAtts, defs []QueryTableDefault) []ColumnDefinition {
	colDefs := make([]ColumnDefinition, 0)
	/*
	 * The queries to get attributes and defaults ORDER BY oid and then attribute
	 * number, so we can assume the arrays are in the same order without sorting.
	 */
	j := 0
	for i := range atts {
		defaultVal := ""
		if atts[i].AttHasDefault {
			for j < len(defs) {
				if atts[i].AttNum == defs[j].AdNum {
					defaultVal = defs[j].DefaultVal
					break
				}
				j++
			}
		}
		colDef := ColumnDefinition{
			Num:        atts[i].AttNum,
			Name:       atts[i].AttName,
			NotNull:    atts[i].AttNotNull,
			HasDefault: atts[i].AttHasDefault,
			IsDropped:  atts[i].AttIsDropped,
			TypName:    atts[i].AttTypName,
			Encoding:   atts[i].AttEncoding,
			Comment:    atts[i].AttComment,
			DefaultVal: defaultVal,
		}
		colDefs = append(colDefs, colDef)
	}
	return colDefs
}

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func ProcessConstraints(table utils.Relation, constraints []QueryConstraint) ([]string, []string) {
	alterStr := fmt.Sprintf("\n\nALTER TABLE ONLY %s ADD CONSTRAINT %s %s;", table.ToString(), "%s", "%s")
	commentStr := fmt.Sprintf("\n\nCOMMENT ON CONSTRAINT %s ON %s IS '%s';", "%s", table.ToString(), "%s")
	cons := make([]string, 0)
	fkCons := make([]string, 0)
	for _, constraint := range constraints {
		conStr := fmt.Sprintf(alterStr, constraint.ConName, constraint.ConDef)
		if constraint.ConComment != "" {
			conStr += fmt.Sprintf(commentStr, constraint.ConName, constraint.ConComment)
		}
		if constraint.ConType == "f" {
			fkCons = append(fkCons, conStr)
		} else {
			cons = append(cons, conStr)
		}
	}
	return cons, fkCons
}

func PrintCreateDatabaseStatement(predataFile io.Writer) {
	dbname := utils.QuoteIdent(connection.DBName)
	owner := utils.QuoteIdent(GetDatabaseOwner(connection))
	fmt.Fprintf(predataFile, "\n\nCREATE DATABASE %s;", dbname)
	fmt.Fprintf(predataFile, "\nALTER DATABASE %s OWNER TO %s;", dbname, owner)
}

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	if tableDef.IsExternal {
		PrintExternalTableCreateStatement(predataFile, table, tableDef)
	} else {
		PrintRegularTableCreateStatement(predataFile, table, tableDef)
	}
	PrintPostCreateTableStatements(predataFile, table, tableDef)
}

func PrintRegularTableCreateStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	fmt.Fprintf(predataFile, "\n\nCREATE TABLE %s (\n", table.ToString())
	printColumnStatements(predataFile, table, tableDef.ColumnDefs)
	fmt.Fprintf(predataFile, ") ")
	if tableDef.StorageOpts != "" {
		fmt.Fprintf(predataFile, "WITH (%s) ", tableDef.StorageOpts)
	}
	fmt.Fprintf(predataFile, "%s", tableDef.DistPolicy)
	if tableDef.PartDef != "" {
		fmt.Fprintf(predataFile, " %s", strings.TrimSpace(tableDef.PartDef))
	}
	fmt.Fprintln(predataFile, ";")
	if tableDef.PartTemplateDef != "" {
		fmt.Fprintf(predataFile, "%s;\n", strings.TrimSpace(tableDef.PartTemplateDef))
	}
}

func PrintExternalTableCreateStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	tableTypeStrMap := map[int]string{
		READABLE: "READABLE EXTERNAL",
		READABLE_WEB: "READABLE EXTERNAL WEB",
		WRITABLE: "WRITABLE EXTERNAL",
		WRITABLE_WEB: "WRITABLE EXTERNAL WEB",
	}
	extTableDef := tableDef.ExtTableDef
	extTableDef.Type, extTableDef.Protocol = DetermineExternalTableCharacteristics(extTableDef)
	fmt.Fprintf(predataFile, "\n\nCREATE %s TABLE %s (\n", tableTypeStrMap[extTableDef.Type], table.ToString())
	printColumnStatements(predataFile, table, tableDef.ColumnDefs)
	fmt.Fprintf(predataFile, ") ")
	PrintExternalTableStatements(predataFile, table, extTableDef)
	fmt.Fprintf(predataFile, "%s;", tableDef.DistPolicy)
}

const (
	// Type of external table
	READABLE = iota
	READABLE_WEB
	WRITABLE
	WRITABLE_WEB
	// Protocol external table is using
	FILE
	GPFDIST
	GPHDFS
	HTTP
	S3
)

func DetermineExternalTableCharacteristics(extTableDef ExternalTableDefinition) (int, int) {
	isWritable := extTableDef.Writable
	tableType := -1
	tableProtocol := -1
	if extTableDef.Location == "" { // EXTERNAL WEB tables may have EXECUTE instead of LOCATION
		tableProtocol = HTTP
		if isWritable {
			tableType = WRITABLE_WEB
		} else {
			tableType = READABLE_WEB
		}
	} else {
		isWeb := strings.HasPrefix(extTableDef.Location, "http")
		if isWeb && isWritable{
			tableType = WRITABLE_WEB
		} else if isWeb && !isWritable {
			tableType = READABLE_WEB
		} else if !isWeb && isWritable{
			tableType = WRITABLE
		} else {
			tableType = READABLE
		}
		prefix := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
		switch prefix {
			case "file": tableProtocol = FILE
			case "gpfdist": tableProtocol = GPFDIST
			case "gpfdists": tableProtocol = GPFDIST
			case "gphdfs": tableProtocol = GPHDFS
			case "http": tableProtocol = HTTP
			case "https": tableProtocol = HTTP
			case "s3": tableProtocol = S3
		}
	}
	return tableType, tableProtocol
}

func printColumnStatements(predataFile io.Writer, table utils.Relation, columnDefs []ColumnDefinition) {
	lines := make([]string, 0)
	for _, column := range columnDefs {
		if !column.IsDropped {
			line := fmt.Sprintf("\t%s %s", utils.QuoteIdent(column.Name), column.TypName)
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
		fmt.Fprintln(predataFile, strings.Join(lines, ",\n"))
	}
}

func PrintExternalTableStatements(predataFile io.Writer, table utils.Relation, extTableDef ExternalTableDefinition) {
	if extTableDef.Type != WRITABLE_WEB {
		if extTableDef.Location != "" {
			locations := make([]string, 0)
			for _, loc := range strings.Split(extTableDef.Location, ",") {
				locations = append(locations, fmt.Sprintf("\t'%s'", loc))
			}
			fmt.Fprintf(predataFile, "LOCATION (\n%s\n)", strings.Join(locations, "\n"))
		}
	}
	if extTableDef.Type == READABLE || (extTableDef.Type == WRITABLE_WEB && extTableDef.Protocol == S3) {
		if extTableDef.ExecLocation == "MASTER_ONLY" {
			fmt.Fprintf(predataFile, " ON MASTER")
		}
	}
	if extTableDef.Type == READABLE_WEB || extTableDef.Type == WRITABLE_WEB {
		if extTableDef.Command != "" {
			fmt.Fprintf(predataFile, "EXECUTE '%s'", extTableDef.Command)
			execType := strings.Split(extTableDef.ExecLocation, ":")
			switch execType[0] {
				case "ALL_SEGMENTS": // Default case, don't print anything else
				case "HOST": fmt.Fprintf(predataFile, " ON HOST '%s'", execType[1])
				case "MASTER_ONLY": fmt.Fprintf(predataFile, " ON MASTER")
				case "PER_HOST": fmt.Fprintf(predataFile, " ON HOST")
				case "SEGMENT_ID": fmt.Fprintf(predataFile, " ON SEGMENT %s", execType[1])
				case "TOTAL_SEGS": fmt.Fprintf(predataFile, " ON %s", execType[1])
			}
		}
	}
	fmt.Fprintln(predataFile)
	formatType := ""
	switch extTableDef.FormatType {
		case "a": formatType = "avro"
		case "b": formatType = "custom"
		case "c": formatType = "csv"
		case "p": formatType = "parquet"
		case "t": formatType = "text"
	}
	/*
	 * The options for the custom formatter is stored as "formatter 'function_name'",
	 * but FORMAT requires "formatter='function_name'".
	 */
	extTableDef.FormatOpts = strings.Replace(extTableDef.FormatOpts, "formatter ", "formatter=", 1)
	fmt.Fprintf(predataFile, "FORMAT '%s'", formatType)
	if extTableDef.FormatOpts != "" {
		fmt.Fprintf(predataFile, " (%s)", strings.TrimSpace(extTableDef.FormatOpts))
	}
	fmt.Fprintln(predataFile)
	if extTableDef.Options != "" {
		fmt.Fprintf(predataFile, "OPTIONS (\n\t%s\n)\n", extTableDef.Options)
	}
	fmt.Fprintf(predataFile, "ENCODING '%s'\n", extTableDef.Encoding)
	if extTableDef.Type == READABLE || extTableDef.Type == READABLE_WEB {
		/*
		 * In GPDB 5 and later, LOG ERRORS INTO [table] has been replaced by LOG ERRORS,
		 * but it still uses the same catalog entries to store that information.  If the
		 * value of pg_exttable.fmterrtbl matches the table's own name, LOG ERRORS is set.
		 */
		if extTableDef.ErrTable == table.RelationName {
			fmt.Fprintln(predataFile, "LOG ERRORS")
		}
		if extTableDef.RejectLimit != 0 {
			fmt.Fprintf(predataFile, "SEGMENT REJECT LIMIT %d ", extTableDef.RejectLimit)
			switch extTableDef.RejectLimitType {
				case "r": fmt.Fprintln(predataFile, "ROWS")
				case "p": fmt.Fprintln(predataFile, "PERCENT")
			}
		}
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	if table.Comment != ""{
		fmt.Fprintf(predataFile, "\n\nCOMMENT ON TABLE %s IS '%s';\n", table.ToString(), table.Comment)
	}
	if table.Owner != "" {
		fmt.Fprintf(predataFile, "\n\nALTER TABLE %s OWNER TO %s;\n", table.ToString(), utils.QuoteIdent(table.Owner))
	}

	for _, att := range tableDef.ColumnDefs {
		if att.Comment != "" {
			fmt.Fprintf(predataFile, "\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.ToString(), att.Name, att.Comment)
		}
	}
}

func PrintConstraintStatements(predataFile io.Writer, constraints []string, fkConstraints []string) {
	sort.Strings(constraints)
	sort.Strings(fkConstraints)
	for _, constraint := range constraints {
		fmt.Fprintln(predataFile, constraint)
	}
	for _, constraint := range fkConstraints {
		fmt.Fprintln(predataFile, constraint)
	}
}

func PrintCreateSchemaStatements(predataFile io.Writer, schemas []utils.Schema) {
	for _, schema := range schemas {
		fmt.Fprintln(predataFile)
		if schema.SchemaName != "public" {
			fmt.Fprintf(predataFile, "\nCREATE SCHEMA %s;", schema.ToString())
		}
		if schema.Owner != "" {
			fmt.Fprintf(predataFile, "\nALTER SCHEMA %s OWNER TO %s;", schema.ToString(), utils.QuoteIdent(schema.Owner))
		}
		if schema.Comment != "" {
			fmt.Fprintf(predataFile, "\nCOMMENT ON SCHEMA %s IS '%s';", schema.ToString(), schema.Comment)
		}
	}
}

func GetAllSequenceDefinitions(connection *utils.DBConn) []SequenceDefinition {
	allSequences := GetAllSequences(connection)
	sequenceDefs := make([]SequenceDefinition, 0)
	for _, seq := range allSequences {
		sequence := GetSequence(connection, seq.ToString())
		sequenceDef := SequenceDefinition{seq, sequence}
		sequenceDefs = append(sequenceDefs, sequenceDef)
	}
	return sequenceDefs
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(predataFile io.Writer, sequences []SequenceDefinition) {
	maxVal := int64(9223372036854775807)
	minVal := int64(-9223372036854775807)
	for _, sequence := range sequences {
		fmt.Fprintln(predataFile, "\n\nCREATE SEQUENCE", sequence.ToString())
		if !sequence.IsCalled {
			fmt.Fprintln(predataFile, "\tSTART WITH", sequence.LastVal)
		}
		fmt.Fprintln(predataFile, "\tINCREMENT BY", sequence.Increment)

		if !((sequence.MaxVal == maxVal && sequence.Increment > 0) || (sequence.MaxVal == -1 && sequence.Increment < 0)) {
			fmt.Fprintln(predataFile, "\tMAXVALUE", sequence.MaxVal)
		} else {
			fmt.Fprintln(predataFile, "\tNO MAXVALUE")
		}
		if !((sequence.MinVal == minVal && sequence.Increment < 0) || (sequence.MinVal == 1 && sequence.Increment > 0)) {
			fmt.Fprintln(predataFile, "\tMINVALUE", sequence.MinVal)
		} else {
			fmt.Fprintln(predataFile, "\tNO MINVALUE")
		}
		cycleStr := ""
		if sequence.IsCycled {
			cycleStr = "\n\tCYCLE"
		}
		fmt.Fprintf(predataFile, "\tCACHE %d%s;", sequence.CacheVal, cycleStr)

		fmt.Fprintf(predataFile, "\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", sequence.ToString(), sequence.LastVal, sequence.IsCalled)

		if sequence.Owner != "" {
			fmt.Fprintf(predataFile, "\n\nALTER TABLE %s OWNER TO %s;\n", sequence.ToString(), utils.QuoteIdent(sequence.Owner))
		}

		if sequence.Comment != "" {
			fmt.Fprintf(predataFile, "\n\nCOMMENT ON SEQUENCE %s IS '%s';\n", sequence.ToString(), sequence.Comment)
		}
	}
}

func PrintSessionGUCs(predataFile io.Writer, gucs QuerySessionGUCs) {
	fmt.Fprintf(predataFile, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET client_encoding = '%s';
SET standard_conforming_strings = %s;
SET default_with_oids = %s;
`, gucs.ClientEncoding, gucs.StdConformingStrings, gucs.DefaultWithOids)
}

func PrintDatabaseGUCs(predataFile io.Writer, gucs []string, dbname string) {
	for _, guc := range gucs {
		fmt.Fprintf(predataFile, "\nALTER DATABASE %s SET %s;", dbname, guc)
	}
}

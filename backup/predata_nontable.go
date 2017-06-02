package backup

/*
 * This file contains structs and functions related to dumping non-table-related
 * metadata on the master that needs to be restored before data is restored, such
 * as sequences and check constraints.
 */

import (
	"fmt"
	"gpbackup/utils"
	"io"
	"sort"
	"strings"
)

type SequenceDefinition struct {
	utils.Relation
	QuerySequence
}

/*
 * Functions to print to the predata file
 */

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

func PrintCreateLanguageStatements(predataFile io.Writer, procLangs []QueryProceduralLanguage) {
	for _, procLang := range procLangs {
		quotedOwner := utils.QuoteIdent(procLang.Owner)
		quotedLanguage := utils.QuoteIdent(procLang.Name)
		fmt.Fprintf(predataFile, "\n\nCREATE ")
		if procLang.PlTrusted {
			fmt.Fprintf(predataFile, "TRUSTED ")
		}
		fmt.Fprintf(predataFile, "PROCEDURAL LANGUAGE %s;", quotedLanguage)
		/*
		 * If the handler, validator, and inline functions are in pg_pltemplate, we can
		 * dump a CREATE LANGUAGE command without specifying them individually.
		 *
		 * The schema of the handler function should match the schema of the language itself, but
		 * the inline and validator functions can be in a different schema and must be schema-qualified.
		 */

		if procLang.Handler != "" {
			fmt.Fprintf(predataFile, "\nALTER FUNCTION %s OWNER TO %s;", procLang.Handler, quotedOwner)
		}
		if procLang.Inline != "" {
			fmt.Fprintf(predataFile, "\nALTER FUNCTION %s OWNER TO %s;", procLang.Inline, quotedOwner)
		}
		if procLang.Validator != "" {
			fmt.Fprintf(predataFile, "\nALTER FUNCTION %s OWNER TO %s;", procLang.Validator, quotedOwner)
		}
		if procLang.Owner != "" {
			fmt.Fprintf(predataFile, "\nALTER LANGUAGE %s OWNER TO %s;", quotedLanguage, quotedOwner)
		}
		if procLang.Comment != "" {
			fmt.Fprintf(predataFile, "\n\nCOMMENT ON LANGUAGE %s IS '%s';", quotedLanguage, procLang.Comment)
		}
		fmt.Fprintln(predataFile)
	}
}

func PrintCreateFunctionStatements(predataFile io.Writer, funcDefs []QueryFunctionDefinition) {
	for _, funcDef := range funcDefs {
		funcFQN := fmt.Sprintf("%s.%s", utils.QuoteIdent(funcDef.SchemaName), utils.QuoteIdent(funcDef.FunctionName))
		fmt.Fprintf(predataFile, "\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
		if funcDef.ReturnsSet && !strings.HasPrefix(funcDef.ResultType, "TABLE") {
			fmt.Fprintf(predataFile, "SETOF ")
		}
		fmt.Fprintf(predataFile, "%s AS ", funcDef.ResultType)
		PrintFunctionBodyOrPath(predataFile, funcDef)
		fmt.Fprintf(predataFile, "LANGUAGE %s", funcDef.Language)
		PrintFunctionModifiers(predataFile, funcDef)
		fmt.Fprintln(predataFile, ";")

		if funcDef.Owner != "" {
			fmt.Fprintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;\n", funcFQN, funcDef.IdentArgs, utils.QuoteIdent(funcDef.Owner))
		}
		if funcDef.Comment != "" {
			fmt.Fprintf(predataFile, "\nCOMMENT ON FUNCTION %s(%s) IS '%s';\n", funcFQN, funcDef.IdentArgs, funcDef.Comment)
		}
	}
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(predataFile io.Writer, funcDef QueryFunctionDefinition) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		fmt.Fprintf(predataFile, "\n%s, %s\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		formattedStr := fmt.Sprintf("\n%s\n", strings.TrimSpace(funcDef.FunctionBody))
		if funcDef.Language == "internal" {
			/*
			 * We assume here that if funcDef.Language is 'internal' then
			 * funcDef.functionBody holds the name of a function to call rather
			 * than a function definition.  For any other language, it is a function
			 * definition and must be quoted appropriately.
			 */
			fmt.Fprintf(predataFile, formattedStr)
		} else {
			fmt.Fprintf(predataFile, "%s ", utils.DollarQuoteString(formattedStr))
		}
	}
}

func PrintFunctionModifiers(predataFile io.Writer, funcDef QueryFunctionDefinition) {
	switch funcDef.SqlUsage {
	case "c":
		fmt.Fprint(predataFile, " CONTAINS SQL")
	case "m":
		fmt.Fprint(predataFile, " MODIFIES SQL DATA")
	case "n":
		fmt.Fprint(predataFile, " NO SQL")
	case "r":
		fmt.Fprint(predataFile, " READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		fmt.Fprintf(predataFile, " IMMUTABLE")
	case "s":
		fmt.Fprintf(predataFile, " STABLE")
	case "v": // Default case, don't print anything else
	}
	if funcDef.IsStrict {
		fmt.Fprintf(predataFile, " STRICT")
	}
	if funcDef.IsSecurityDefiner {
		fmt.Fprintf(predataFile, " SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1)) {
		fmt.Fprintf(predataFile, "\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		fmt.Fprintf(predataFile, "\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		fmt.Fprintf(predataFile, "\n%s", funcDef.Config)
	}
}

/*
 * Functions to print to the global or postdata file instead of, or in addition
 * to, the predata file.
 */

func PrintConnectionString(metadataFile io.Writer, dbname string) {
	fmt.Fprintf(metadataFile, "\\c %s\n", dbname)
}

func PrintSessionGUCs(metadataFile io.Writer, gucs QuerySessionGUCs) {
	fmt.Fprintf(metadataFile, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = '%s';
SET standard_conforming_strings = %s;
SET default_with_oids = %s;
`, gucs.ClientEncoding, gucs.StdConformingStrings, gucs.DefaultWithOids)
}

func PrintCreateDatabaseStatement(globalFile io.Writer) {
	dbname := utils.QuoteIdent(connection.DBName)
	owner := utils.QuoteIdent(GetDatabaseOwner(connection))
	fmt.Fprintf(globalFile, "\n\nCREATE DATABASE %s;", dbname)
	fmt.Fprintf(globalFile, "\nALTER DATABASE %s OWNER TO %s;", dbname, owner)
}

func PrintDatabaseGUCs(globalFile io.Writer, gucs []string, dbname string) {
	for _, guc := range gucs {
		fmt.Fprintf(globalFile, "\nALTER DATABASE %s SET %s;", dbname, guc)
	}
}

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
)

type SequenceDefinition struct {
	utils.Relation
	QuerySequence
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
			fmt.Fprintf(predataFile, "\n\nCOMMENT ON LANGUAGE %s IS '%s';\n", quotedLanguage, procLang.Comment)
		}
	}
}

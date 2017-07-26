package backup

/*
 * This file contains structs and functions related to dumping metadata on the
 * master for objects that don't fall under any other predata categorization,
 * such as procedural languages and constraints, that needs to be restored
 * before data is restored.
 */

import (
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func PrintConstraintStatements(predataFile io.Writer, constraints []QueryConstraint, conMetadata MetadataMap) {
	allConstraints := make([]QueryConstraint, 0)
	allFkConstraints := make([]QueryConstraint, 0)
	/*
	 * Because FOREIGN KEY constraints must be dumped after PRIMARY KEY
	 * constraints, we separate the two types then concatenate the lists,
	 * so FOREIGN KEY are guaranteed to be printed last.
	 */
	for _, constraint := range constraints {
		if constraint.ConType == "f" {
			allFkConstraints = append(allFkConstraints, constraint)
		} else {
			allConstraints = append(allConstraints, constraint)
		}
	}
	constraints = append(allConstraints, allFkConstraints...)

	alterStr := "\n\nALTER %s %s ADD CONSTRAINT %s %s;\n"
	for _, constraint := range constraints {
		objStr := "TABLE ONLY"
		if constraint.IsDomainConstraint {
			objStr = "DOMAIN"
		}
		conName := utils.QuoteIdent(constraint.ConName)
		utils.MustPrintf(predataFile, alterStr, objStr, constraint.OwningObject, conName, constraint.ConDef)
		PrintObjectMetadata(predataFile, conMetadata[constraint.Oid], conName, "CONSTRAINT", constraint.OwningObject)
	}
}

func PrintCreateSchemaStatements(predataFile io.Writer, schemas []utils.Schema, schemaMetadata MetadataMap) {
	for _, schema := range schemas {
		utils.MustPrintln(predataFile)
		if schema.Name != "public" {
			utils.MustPrintf(predataFile, "\nCREATE SCHEMA %s;", schema.ToString())
		}
		PrintObjectMetadata(predataFile, schemaMetadata[schema.Oid], schema.ToString(), "SCHEMA")
	}
}

/*
 * This function separates out functions related to procedural languages from
 * any other functions, so that language-related functions can be dumped before
 * the languages themselves and we can avoid sorting languages and functions
 * together to resolve dependencies.
 */
func ExtractLanguageFunctions(funcDefs []QueryFunctionDefinition, procLangs []QueryProceduralLanguage) ([]QueryFunctionDefinition, []QueryFunctionDefinition) {
	isLangFuncMap := make(map[uint32]bool, 0)
	for _, procLang := range procLangs {
		for _, funcDef := range funcDefs {
			isLangFuncMap[funcDef.Oid] = (funcDef.Oid == procLang.Handler ||
				funcDef.Oid == procLang.Inline ||
				funcDef.Oid == procLang.Validator)
		}
	}
	langFuncs := make([]QueryFunctionDefinition, 0)
	otherFuncs := make([]QueryFunctionDefinition, 0)
	for _, funcDef := range funcDefs {
		if isLangFuncMap[funcDef.Oid] {
			langFuncs = append(langFuncs, funcDef)
		} else {
			otherFuncs = append(otherFuncs, funcDef)
		}
	}
	return langFuncs, otherFuncs
}

func PrintCreateLanguageStatements(predataFile io.Writer, procLangs []QueryProceduralLanguage,
	funcInfoMap map[uint32]FunctionInfo, procLangMetadata MetadataMap) {
	for _, procLang := range procLangs {
		quotedOwner := utils.QuoteIdent(procLang.Owner)
		quotedLanguage := utils.QuoteIdent(procLang.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE ")
		if procLang.PlTrusted {
			utils.MustPrintf(predataFile, "TRUSTED ")
		}
		utils.MustPrintf(predataFile, "PROCEDURAL LANGUAGE %s;", quotedLanguage)
		/*
		 * If the handler, validator, and inline functions are in pg_pltemplate, we can
		 * dump a CREATE LANGUAGE command without specifying them individually.
		 *
		 * The schema of the handler function should match the schema of the language itself, but
		 * the inline and validator functions can be in a different schema and must be schema-qualified.
		 */

		if procLang.Handler != 0 {
			handlerInfo := funcInfoMap[procLang.Handler]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", handlerInfo.QualifiedName, handlerInfo.Arguments, quotedOwner)
		}
		if procLang.Inline != 0 {
			inlineInfo := funcInfoMap[procLang.Inline]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", inlineInfo.QualifiedName, inlineInfo.Arguments, quotedOwner)
		}
		if procLang.Validator != 0 {
			validatorInfo := funcInfoMap[procLang.Validator]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", validatorInfo.QualifiedName, validatorInfo.Arguments, quotedOwner)
		}
		PrintObjectMetadata(predataFile, procLangMetadata[procLang.Oid], utils.QuoteIdent(procLang.Name), "LANGUAGE")
		utils.MustPrintln(predataFile)
	}
}

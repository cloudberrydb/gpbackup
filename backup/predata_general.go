package backup

/*
 * This file contains structs and functions related to dumping metadata on the
 * master for objects that don't fall under any other predata categorization,
 * such as procedural languages and constraints, that needs to be restored
 * before data is restored.
 */

import (
	"fmt"
	"io"
	"strings"

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
		} else if constraint.IsPartitionParent {
			objStr = "TABLE"
		}
		conName := utils.QuoteIdent(constraint.ConName)
		utils.MustPrintf(predataFile, alterStr, objStr, constraint.OwningObject, conName, constraint.ConDef)
		PrintObjectMetadata(predataFile, conMetadata[constraint.Oid], conName, "CONSTRAINT", constraint.OwningObject)
	}
}

func PrintCreateSchemaStatements(predataFile io.Writer, schemas []Schema, schemaMetadata MetadataMap) {
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

func PrintCreateOperatorStatements(predataFile io.Writer, operators []QueryOperator, operatorMetadata MetadataMap) {
	for _, operator := range operators {
		// We do not use MakeFQN here as the operator cannot be quoted
		schema := utils.QuoteIdent(operator.SchemaName)
		operatorFQN := fmt.Sprintf("%s.%s", schema, operator.Name)
		optionalFields := make([]string, 0)
		leftArg := "NONE"
		rightArg := "NONE"
		if operator.LeftArgType != "-" {
			leftArg = operator.LeftArgType
			optionalFields = append(optionalFields, fmt.Sprintf("LEFTARG = %s", leftArg))
		}
		if operator.RightArgType != "-" {
			rightArg = operator.RightArgType
			optionalFields = append(optionalFields, fmt.Sprintf("RIGHTARG = %s", rightArg))
		}
		if operator.CommutatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("COMMUTATOR = OPERATOR(%s)", operator.CommutatorOp))
		}
		if operator.NegatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("NEGATOR = OPERATOR(%s)", operator.NegatorOp))
		}
		if operator.RestrictFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("RESTRICT = %s", operator.RestrictFunction))
		}
		if operator.JoinFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("JOIN = %s", operator.JoinFunction))
		}
		if operator.CanHash {
			optionalFields = append(optionalFields, "HASHES")
		}
		if operator.CanMerge {
			optionalFields = append(optionalFields, "MERGES")
		}
		utils.MustPrintf(predataFile, `

CREATE OPERATOR %s (
	PROCEDURE = %s,
	%s
);`, operatorFQN, operator.ProcedureName, strings.Join(optionalFields, ",\n\t"))
		operatorStr := fmt.Sprintf("%s (%s, %s)", operatorFQN, leftArg, rightArg)
		PrintObjectMetadata(predataFile, operatorMetadata[operator.Oid], operatorStr, "OPERATOR")
	}
}
func PrintCreateOperatorFamilyStatements(predataFile io.Writer, operatorFamilies []QueryOperatorFamily, operatorFamilyMetadata MetadataMap) {
	for _, operatorFamily := range operatorFamilies {
		operatorFamilyFQN := MakeFQN(operatorFamily.SchemaName, operatorFamily.Name)
		operatorFamilyStr := fmt.Sprintf("%s USING %s", operatorFamilyFQN, utils.QuoteIdent(operatorFamily.IndexMethod))
		utils.MustPrintf(predataFile, "\n\nCREATE OPERATOR FAMILY %s;", operatorFamilyStr)
		PrintObjectMetadata(predataFile, operatorFamilyMetadata[operatorFamily.Oid], operatorFamilyStr, "OPERATOR FAMILY")
	}
}

func PrintCreateOperatorClassStatements(predataFile io.Writer, operatorClasses []QueryOperatorClass, operatorClassMetadata MetadataMap) {
	for _, operatorClass := range operatorClasses {
		operatorClassFQN := MakeFQN(operatorClass.ClassSchema, operatorClass.ClassName)
		utils.MustPrintf(predataFile, "\n\nCREATE OPERATOR CLASS %s", operatorClassFQN)
		forTypeStr := ""
		if operatorClass.Default {
			forTypeStr += "DEFAULT "
		}
		forTypeStr += fmt.Sprintf("FOR TYPE %s USING %s", operatorClass.Type, operatorClass.IndexMethod)
		if operatorClass.FamilyName != operatorClass.ClassName {
			operatorFamilyFQN := MakeFQN(operatorClass.FamilySchema, operatorClass.FamilyName)
			forTypeStr += fmt.Sprintf(" FAMILY %s", operatorFamilyFQN)
		}
		utils.MustPrintf(predataFile, "\n\t%s", forTypeStr)
		opClassClauses := []string{}
		if len(operatorClass.Operators) != 0 {
			for _, operator := range operatorClass.Operators {
				opStr := fmt.Sprintf("OPERATOR %d %s", operator.StrategyNumber, operator.Operator)
				if operator.Recheck {
					opStr += " RECHECK"
				}
				opClassClauses = append(opClassClauses, opStr)
			}
		}
		if len(operatorClass.Functions) != 0 {
			for _, function := range operatorClass.Functions {
				opClassClauses = append(opClassClauses, fmt.Sprintf("FUNCTION %d %s", function.SupportNumber, function.FunctionName))
			}
		}
		if operatorClass.StorageType != "-" || len(opClassClauses) == 0 {
			storageType := operatorClass.StorageType
			if operatorClass.StorageType == "-" {
				storageType = operatorClass.Type
			}
			opClassClauses = append(opClassClauses, fmt.Sprintf("STORAGE %s", storageType))
		}
		utils.MustPrintf(predataFile, " AS\n\t%s;", strings.Join(opClassClauses, ",\n\t"))

		operatorClassStr := fmt.Sprintf("%s USING %s", operatorClassFQN, utils.QuoteIdent(operatorClass.IndexMethod))
		PrintObjectMetadata(predataFile, operatorClassMetadata[operatorClass.Oid], operatorClassStr, "OPERATOR CLASS")
	}
}

func PrintCreateConversionStatements(predataFile io.Writer, conversions []Conversion, conversionMetadata MetadataMap) {
	for _, conversion := range conversions {
		convFQN := MakeFQN(conversion.Schema, conversion.Name)
		defaultStr := ""
		if conversion.IsDefault {
			defaultStr = " DEFAULT"
		}
		utils.MustPrintf(predataFile, "\n\nCREATE%s CONVERSION %s FOR '%s' TO '%s' FROM %s;",
			defaultStr, convFQN, conversion.ForEncoding, conversion.ToEncoding, conversion.ConversionFunction)
		PrintObjectMetadata(predataFile, conversionMetadata[conversion.Oid], convFQN, "CONVERSION")
		utils.MustPrintln(predataFile)
	}
}

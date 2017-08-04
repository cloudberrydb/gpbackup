package backup

/*
 * This file contains structs and functions related to dumping function
 * metadata, and metadata closely related to functions such as aggregates
 * and casts, that needs to be restored before data is restored.
 */

import (
	"fmt"
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateFunctionStatement(predataFile io.Writer, funcDef Function, funcMetadata ObjectMetadata) {
	funcFQN := MakeFQN(funcDef.SchemaName, funcDef.FunctionName)
	utils.MustPrintf(predataFile, "\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
	utils.MustPrintf(predataFile, "%s AS", funcDef.ResultType)
	PrintFunctionBodyOrPath(predataFile, funcDef)
	utils.MustPrintf(predataFile, "LANGUAGE %s", funcDef.Language)
	PrintFunctionModifiers(predataFile, funcDef)
	utils.MustPrintln(predataFile, ";")

	nameStr := fmt.Sprintf("%s(%s)", funcFQN, funcDef.IdentArgs)
	PrintObjectMetadata(predataFile, funcMetadata, nameStr, "FUNCTION")
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(predataFile io.Writer, funcDef Function) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		utils.MustPrintf(predataFile, "\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		utils.MustPrintf(predataFile, "\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(predataFile io.Writer, funcDef Function) {
	switch funcDef.DataAccess {
	case "c":
		utils.MustPrintf(predataFile, " CONTAINS SQL")
	case "m":
		utils.MustPrintf(predataFile, " MODIFIES SQL DATA")
	case "n":
		utils.MustPrintf(predataFile, " NO SQL")
	case "r":
		utils.MustPrintf(predataFile, " READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		utils.MustPrintf(predataFile, " IMMUTABLE")
	case "s":
		utils.MustPrintf(predataFile, " STABLE")
	case "v": // Default case, don't print anything else
	}
	if funcDef.IsStrict {
		utils.MustPrintf(predataFile, " STRICT")
	}
	if funcDef.IsSecurityDefiner {
		utils.MustPrintf(predataFile, " SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1)) {
		utils.MustPrintf(predataFile, "\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		utils.MustPrintf(predataFile, "\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		utils.MustPrintf(predataFile, "\n%s", funcDef.Config)
	}
}

func PrintCreateAggregateStatements(predataFile io.Writer, aggDefs []Aggregate, funcInfoMap map[uint32]FunctionInfo, aggMetadata MetadataMap) {
	for _, aggDef := range aggDefs {
		aggFQN := MakeFQN(aggDef.SchemaName, aggDef.AggregateName)
		orderedStr := ""
		if aggDef.IsOrdered {
			orderedStr = "ORDERED "
		}
		argumentsStr := "*"
		if aggDef.Arguments != "" {
			argumentsStr = aggDef.Arguments
		}
		utils.MustPrintf(predataFile, "\n\nCREATE %sAGGREGATE %s(%s) (\n", orderedStr, aggFQN, argumentsStr)

		utils.MustPrintf(predataFile, "\tSFUNC = %s,\n", funcInfoMap[aggDef.TransitionFunction].QualifiedName)
		utils.MustPrintf(predataFile, "\tSTYPE = %s", aggDef.TransitionDataType)

		if aggDef.PreliminaryFunction != 0 {
			utils.MustPrintf(predataFile, ",\n\tPREFUNC = %s", funcInfoMap[aggDef.PreliminaryFunction].QualifiedName)
		}
		if aggDef.FinalFunction != 0 {
			utils.MustPrintf(predataFile, ",\n\tFINALFUNC = %s", funcInfoMap[aggDef.FinalFunction].QualifiedName)
		}
		if aggDef.InitialValue != "" {
			utils.MustPrintf(predataFile, ",\n\tINITCOND = '%s'", aggDef.InitialValue)
		}
		if aggDef.SortOperator != 0 {
			utils.MustPrintf(predataFile, ",\n\tSORTOP = %s", funcInfoMap[aggDef.SortOperator].QualifiedName)
		}
		utils.MustPrintln(predataFile, "\n);")

		identArgumentsStr := "*"
		if aggDef.IdentArgs != "" {
			identArgumentsStr = aggDef.IdentArgs
		}
		aggFQN = fmt.Sprintf("%s(%s)", aggFQN, identArgumentsStr)
		PrintObjectMetadata(predataFile, aggMetadata[aggDef.Oid], aggFQN, "AGGREGATE")
	}
}

func PrintCreateCastStatements(predataFile io.Writer, castDefs []Cast, castMetadata MetadataMap) {
	for _, castDef := range castDefs {
		/*
		 * Because we use pg_catalog.format_type() in the query to get the cast definition,
		 * castDef.SourceType and castDef.TargetType are already quoted appropriately.
		 */
		castStr := fmt.Sprintf("(%s AS %s)", castDef.SourceType, castDef.TargetType)
		utils.MustPrintf(predataFile, "\n\nCREATE CAST %s\n", castStr)
		if castDef.FunctionSchema != "" {
			funcFQN := fmt.Sprintf("%s.%s", utils.QuoteIdent(castDef.FunctionSchema), utils.QuoteIdent(castDef.FunctionName))
			utils.MustPrintf(predataFile, "\tWITH FUNCTION %s(%s)", funcFQN, castDef.FunctionArgs)
		} else {
			utils.MustPrintf(predataFile, "\tWITHOUT FUNCTION")
		}
		switch castDef.CastContext {
		case "a":
			utils.MustPrintf(predataFile, "\nAS ASSIGNMENT")
		case "i":
			utils.MustPrintf(predataFile, "\nAS IMPLICIT")
		case "e": // Default case, don't print anything else
		}
		utils.MustPrintf(predataFile, ";")
		PrintObjectMetadata(predataFile, castMetadata[castDef.Oid], castStr, "CAST")
	}
}

/*
 * This function separates out functions related to procedural languages from
 * any other functions, so that language-related functions can be dumped before
 * the languages themselves and we can avoid sorting languages and functions
 * together to resolve dependencies.
 */
func ExtractLanguageFunctions(funcDefs []Function, procLangs []ProceduralLanguage) ([]Function, []Function) {
	isLangFuncMap := make(map[uint32]bool, 0)
	for _, procLang := range procLangs {
		for _, funcDef := range funcDefs {
			isLangFuncMap[funcDef.Oid] = (funcDef.Oid == procLang.Handler ||
				funcDef.Oid == procLang.Inline ||
				funcDef.Oid == procLang.Validator)
		}
	}
	langFuncs := make([]Function, 0)
	otherFuncs := make([]Function, 0)
	for _, funcDef := range funcDefs {
		if isLangFuncMap[funcDef.Oid] {
			langFuncs = append(langFuncs, funcDef)
		} else {
			otherFuncs = append(otherFuncs, funcDef)
		}
	}
	return langFuncs, otherFuncs
}

func PrintCreateLanguageStatements(predataFile io.Writer, procLangs []ProceduralLanguage,
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

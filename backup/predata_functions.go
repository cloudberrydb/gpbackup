package backup

/*
 * This file contains structs and functions related to dumping function
 * metadata, and metadata closely related to functions such as casts, that
 * needs to be restored before data is restored.
 */

import (
	"fmt"
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateFunctionStatements(predataFile io.Writer, funcDefs []QueryFunctionDefinition) {
	for _, funcDef := range funcDefs {
		funcFQN := utils.MakeFQN(funcDef.SchemaName, funcDef.FunctionName)
		utils.MustPrintf(predataFile, "\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
		utils.MustPrintf(predataFile, "%s AS", funcDef.ResultType)
		PrintFunctionBodyOrPath(predataFile, funcDef)
		utils.MustPrintf(predataFile, "LANGUAGE %s", funcDef.Language)
		PrintFunctionModifiers(predataFile, funcDef)
		utils.MustPrintln(predataFile, ";")

		if funcDef.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;\n", funcFQN, funcDef.IdentArgs, utils.QuoteIdent(funcDef.Owner))
		}
		if funcDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON FUNCTION %s(%s) IS '%s';\n", funcFQN, funcDef.IdentArgs, funcDef.Comment)
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
		utils.MustPrintf(predataFile, "\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		utils.MustPrintf(predataFile, "\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(predataFile io.Writer, funcDef QueryFunctionDefinition) {
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

func PrintCreateAggregateStatements(predataFile io.Writer, aggDefs []QueryAggregateDefinition, funcInfoMap map[uint32]FunctionInfo) {
	for _, aggDef := range aggDefs {
		aggFQN := utils.MakeFQN(aggDef.SchemaName, aggDef.AggregateName)
		orderedStr := ""
		if aggDef.IsOrdered {
			orderedStr = "ORDERED "
		}
		utils.MustPrintf(predataFile, "\n\nCREATE %sAGGREGATE %s(%s) (\n", orderedStr, aggFQN, aggDef.Arguments)
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

		if aggDef.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER AGGREGATE %s(%s) OWNER TO %s;\n", aggFQN, aggDef.IdentArgs, utils.QuoteIdent(aggDef.Owner))
		}
		if aggDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON AGGREGATE %s(%s) IS '%s';\n", aggFQN, aggDef.IdentArgs, aggDef.Comment)
		}
	}
}

func PrintCreateCastStatements(predataFile io.Writer, castDefs []QueryCastDefinition) {
	for _, castDef := range castDefs {
		castStr := fmt.Sprintf("CAST (%s AS %s)", castDef.SourceType, castDef.TargetType)
		utils.MustPrintf(predataFile, "\n\nCREATE %s\n", castStr)
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
		utils.MustPrintln(predataFile, ";")
		if castDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON %s IS '%s';\n", castStr, castDef.Comment)
		}
	}
}

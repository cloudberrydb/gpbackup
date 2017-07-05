package backup

/*
 * This file contains structs and functions related to dumping type
 * metadata on the master that needs to be restored before data is restored.
 */

import (
	"fmt"
	"io"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Functions to print to the predata file
 */

/*
 * Because only base types are dependent on functions, we only need to print
 * shell type statements for base types.
 */
func PrintShellTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	utils.MustPrintln(predataFile, "\n")
	for _, typ := range types {
		if typ.Type == "b" || typ.Type == "p" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "CREATE TYPE %s;\n", typeFQN)
		}
	}
}

func PrintCreateBaseTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	i := 0
	for i < len(types) {
		typ := types[i]
		if typ.Type == "b" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s (\n", typeFQN)

			// All of the following functions are stored in quoted form and don't need to be quoted again
			utils.MustPrintf(predataFile, "\tINPUT = %s,\n\tOUTPUT = %s", typ.Input, typ.Output)
			if typ.Receive != "-" {
				utils.MustPrintf(predataFile, ",\n\tRECEIVE = %s", typ.Receive)
			}
			if typ.Send != "-" {
				utils.MustPrintf(predataFile, ",\n\tSEND = %s", typ.Send)
			}
			if typ.ModIn != "-" {
				utils.MustPrintf(predataFile, ",\n\tTYPMOD_IN = %s", typ.ModIn)
			}
			if typ.ModOut != "-" {
				utils.MustPrintf(predataFile, ",\n\tTYPMOD_OUT = %s", typ.ModOut)
			}
			if typ.InternalLength > 0 {
				utils.MustPrintf(predataFile, ",\n\tINTERNALLENGTH = %d", typ.InternalLength)
			}
			if typ.IsPassedByValue {
				utils.MustPrintf(predataFile, ",\n\tPASSEDBYVALUE")
			}
			if typ.Alignment != "-" {
				switch typ.Alignment {
				case "d":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = double")
				case "i":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int4")
				case "s":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int2")
				case "c": // Default case, don't print anything else
				}
			}
			if typ.Storage != "" {
				switch typ.Storage {
				case "e":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = extended")
				case "m":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = main")
				case "x":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = external")
				case "p": // Default case, don't print anything else
				}
			}
			if typ.DefaultVal != "" {
				utils.MustPrintf(predataFile, ",\n\tDEFAULT = %s", typ.DefaultVal)
			}
			if typ.Element != "-" {
				utils.MustPrintf(predataFile, ",\n\tELEMENT = %s", typ.Element)
			}
			if typ.Delimiter != "" {
				utils.MustPrintf(predataFile, ",\n\tDELIMITER = '%s'", typ.Delimiter)
			}
			utils.MustPrintln(predataFile, "\n);")
			if typ.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, typ.Comment)
			}
			if typ.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, typ.Owner)
			}
		}
		i++
	}
}

func PrintCreateCompositeAndEnumTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	i := 0
	for i < len(types) {
		typ := types[i]
		if typ.Type == "c" {
			compositeTypes := make([]TypeDefinition, 0)
			/*
			 * Since types is sorted by schema then by type, all TypeDefinitions
			 * for the same composite type are grouped together.  Collect them in
			 * one list to use for printing
			 */
			for {
				if i < len(types) && typ.TypeSchema == types[i].TypeSchema && typ.TypeName == types[i].TypeName {
					compositeTypes = append(compositeTypes, types[i])
					i++
				} else {
					break
				}
			}
			/*
			 * All values except AttName and AttType will be the same for each TypeDefinition,
			 * so we can grab all other values from the first TypeDefinition in the list.
			 */
			composite := compositeTypes[0]
			typeFQN := utils.MakeFQN(composite.TypeSchema, composite.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS (\n", typeFQN)
			atts := make([]string, 0)
			for _, composite := range compositeTypes {
				atts = append(atts, fmt.Sprintf("\t%s %s", composite.AttName, composite.AttType))
			}
			utils.MustPrintf(predataFile, strings.Join(atts, ",\n"))
			utils.MustPrintln(predataFile, "\n);")
			if composite.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, composite.Comment)
			}
			if composite.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, utils.QuoteIdent(composite.Owner))
			}
		} else if typ.Type == "e" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, typ.EnumLabels)
			if typ.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, typ.Comment)
			}
			if typ.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, utils.QuoteIdent(typ.Owner))
			}
			i++

		} else {
			i++
		}
	}
}

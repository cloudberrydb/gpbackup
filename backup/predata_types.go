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
func PrintCreateShellTypeStatements(predataFile io.Writer, types []Type) {
	utils.MustPrintln(predataFile, "\n")
	for _, typ := range types {
		if typ.Type == "b" || typ.Type == "p" {
			typeFQN := MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "CREATE TYPE %s;\n", typeFQN)
		}
	}
}

func PrintCreateDomainStatement(predataFile io.Writer, domain Type, typeMetadata ObjectMetadata, constraints []Constraint) {
	typeFQN := MakeFQN(domain.TypeSchema, domain.TypeName)
	utils.MustPrintf(predataFile, "\nCREATE DOMAIN %s AS %s", typeFQN, domain.BaseType)
	if domain.DefaultVal != "" {
		utils.MustPrintf(predataFile, " DEFAULT %s", domain.DefaultVal)
	}
	if domain.NotNull {
		utils.MustPrintf(predataFile, " NOT NULL")
	}
	for _, constraint := range constraints {
		utils.MustPrintf(predataFile, "\n\tCONSTRAINT %s %s", constraint.ConName, constraint.ConDef)
	}
	utils.MustPrintln(predataFile, ";")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "DOMAIN")
}

func PrintCreateBaseTypeStatement(predataFile io.Writer, base Type, typeMetadata ObjectMetadata) {
	typeFQN := MakeFQN(base.TypeSchema, base.TypeName)
	utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s (\n", typeFQN)

	// All of the following functions are stored in quoted form and don't need to be quoted again
	utils.MustPrintf(predataFile, "\tINPUT = %s,\n\tOUTPUT = %s", base.Input, base.Output)
	if base.Receive != "" {
		utils.MustPrintf(predataFile, ",\n\tRECEIVE = %s", base.Receive)
	}
	if base.Send != "" {
		utils.MustPrintf(predataFile, ",\n\tSEND = %s", base.Send)
	}
	if base.ModIn != "" {
		utils.MustPrintf(predataFile, ",\n\tTYPMOD_IN = %s", base.ModIn)
	}
	if base.ModOut != "" {
		utils.MustPrintf(predataFile, ",\n\tTYPMOD_OUT = %s", base.ModOut)
	}
	if base.InternalLength > 0 {
		utils.MustPrintf(predataFile, ",\n\tINTERNALLENGTH = %d", base.InternalLength)
	}
	if base.IsPassedByValue {
		utils.MustPrintf(predataFile, ",\n\tPASSEDBYVALUE")
	}
	if base.Alignment != "" {
		switch base.Alignment {
		case "d":
			utils.MustPrintf(predataFile, ",\n\tALIGNMENT = double")
		case "i":
			utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int4")
		case "s":
			utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int2")
		case "c": // Default case, don't print anything else
		}
	}
	if base.Storage != "" {
		switch base.Storage {
		case "e":
			utils.MustPrintf(predataFile, ",\n\tSTORAGE = extended")
		case "m":
			utils.MustPrintf(predataFile, ",\n\tSTORAGE = main")
		case "x":
			utils.MustPrintf(predataFile, ",\n\tSTORAGE = external")
		case "p": // Default case, don't print anything else
		}
	}
	if base.DefaultVal != "" {
		utils.MustPrintf(predataFile, ",\n\tDEFAULT = '%s'", base.DefaultVal)
	}
	if base.Element != "" {
		utils.MustPrintf(predataFile, ",\n\tELEMENT = %s", base.Element)
	}
	if base.Delimiter != "" {
		utils.MustPrintf(predataFile, ",\n\tDELIMITER = '%s'", base.Delimiter)
	}
	utils.MustPrintln(predataFile, "\n);")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "TYPE")
}

type CompositeType struct {
	AttName string
	AttType string
}

func CoalesceCompositeTypes(types []Type) []Type {
	i := 0
	coalescedTypes := make([]Type, 0)
	for i < len(types) {
		typ := types[i]
		if typ.Type == "c" {
			compositeTypes := make([]Type, 0)
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
			atts := make([]CompositeTypeAttribute, 0)
			for _, composite := range compositeTypes {
				atts = append(atts, CompositeTypeAttribute{composite.AttName, composite.AttType})
			}
			composite.CompositeAtts = atts
			coalescedTypes = append(coalescedTypes, composite)
		} else {
			coalescedTypes = append(coalescedTypes, typ)
			i++
		}
	}
	return coalescedTypes
}

func PrintCreateCompositeTypeStatement(predataFile io.Writer, composite Type, typeMetadata ObjectMetadata) {
	typeFQN := MakeFQN(composite.TypeSchema, composite.TypeName)
	utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS (\n", typeFQN)
	atts := make([]string, 0)
	for _, att := range composite.CompositeAtts {
		atts = append(atts, fmt.Sprintf("\t%s %s", att.AttName, att.AttType))
	}
	utils.MustPrintln(predataFile, strings.Join(atts, ",\n"))
	utils.MustPrintf(predataFile, ");")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "TYPE")
}

func PrintCreateEnumTypeStatements(predataFile io.Writer, types []Type, typeMetadata MetadataMap) {
	for _, typ := range types {
		if typ.Type == "e" {
			typeFQN := MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, typ.EnumLabels)
			PrintObjectMetadata(predataFile, typeMetadata[typ.Oid], typeFQN, "TYPE")
		}
	}
}

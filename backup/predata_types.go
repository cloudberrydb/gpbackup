package backup

/*
 * This file contains structs and functions related to dumping type
 * metadata on the master that needs to be restored before data is restored.
 */

import (
	"fmt"
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
func PrintCreateShellTypeStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, types []Type) {
	start := predataFile.ByteCount
	predataFile.MustPrintln("\n")
	for _, typ := range types {
		if typ.Type == "b" || typ.Type == "p" {
			typeFQN := MakeFQN(typ.TypeSchema, typ.TypeName)
			predataFile.MustPrintf("CREATE TYPE %s;\n", typeFQN)
			toc.AddPredataEntry(typ.TypeSchema, typ.TypeName, "TYPE", start, predataFile.ByteCount)
			start = predataFile.ByteCount
		}
	}
}

func PrintCreateDomainStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, domain Type, typeMetadata ObjectMetadata, constraints []Constraint) {
	start := predataFile.ByteCount
	typeFQN := MakeFQN(domain.TypeSchema, domain.TypeName)
	predataFile.MustPrintf("\nCREATE DOMAIN %s AS %s", typeFQN, domain.BaseType)
	if domain.DefaultVal != "" {
		predataFile.MustPrintf(" DEFAULT %s", domain.DefaultVal)
	}
	if domain.NotNull {
		predataFile.MustPrintf(" NOT NULL")
	}
	for _, constraint := range constraints {
		predataFile.MustPrintf("\n\tCONSTRAINT %s %s", constraint.ConName, constraint.ConDef)
	}
	predataFile.MustPrintln(";")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "DOMAIN")
	toc.AddPredataEntry(domain.TypeSchema, domain.TypeName, "DOMAIN", start, predataFile.ByteCount)
}

func PrintCreateBaseTypeStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, base Type, typeMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	typeFQN := MakeFQN(base.TypeSchema, base.TypeName)
	predataFile.MustPrintf("\n\nCREATE TYPE %s (\n", typeFQN)

	// All of the following functions are stored in quoted form and don't need to be quoted again
	predataFile.MustPrintf("\tINPUT = %s,\n\tOUTPUT = %s", base.Input, base.Output)
	if base.Receive != "" {
		predataFile.MustPrintf(",\n\tRECEIVE = %s", base.Receive)
	}
	if base.Send != "" {
		predataFile.MustPrintf(",\n\tSEND = %s", base.Send)
	}
	if base.ModIn != "" {
		predataFile.MustPrintf(",\n\tTYPMOD_IN = %s", base.ModIn)
	}
	if base.ModOut != "" {
		predataFile.MustPrintf(",\n\tTYPMOD_OUT = %s", base.ModOut)
	}
	if base.InternalLength > 0 {
		predataFile.MustPrintf(",\n\tINTERNALLENGTH = %d", base.InternalLength)
	}
	if base.IsPassedByValue {
		predataFile.MustPrintf(",\n\tPASSEDBYVALUE")
	}
	if base.Alignment != "" {
		switch base.Alignment {
		case "d":
			predataFile.MustPrintf(",\n\tALIGNMENT = double")
		case "i":
			predataFile.MustPrintf(",\n\tALIGNMENT = int4")
		case "s":
			predataFile.MustPrintf(",\n\tALIGNMENT = int2")
		case "c": // Default case, don't print anything else
		}
	}
	if base.Storage != "" {
		switch base.Storage {
		case "e":
			predataFile.MustPrintf(",\n\tSTORAGE = extended")
		case "m":
			predataFile.MustPrintf(",\n\tSTORAGE = main")
		case "x":
			predataFile.MustPrintf(",\n\tSTORAGE = external")
		case "p": // Default case, don't print anything else
		}
	}
	if base.DefaultVal != "" {
		predataFile.MustPrintf(",\n\tDEFAULT = '%s'", base.DefaultVal)
	}
	if base.Element != "" {
		predataFile.MustPrintf(",\n\tELEMENT = %s", base.Element)
	}
	if base.Delimiter != "" {
		predataFile.MustPrintf(",\n\tDELIMITER = '%s'", base.Delimiter)
	}
	predataFile.MustPrintln("\n);")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "TYPE")
	toc.AddPredataEntry(base.TypeSchema, base.TypeName, "TYPE", start, predataFile.ByteCount)
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

func PrintCreateCompositeTypeStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, composite Type, typeMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	typeFQN := MakeFQN(composite.TypeSchema, composite.TypeName)
	predataFile.MustPrintf("\n\nCREATE TYPE %s AS (\n", typeFQN)
	atts := make([]string, 0)
	for _, att := range composite.CompositeAtts {
		atts = append(atts, fmt.Sprintf("\t%s %s", att.AttName, att.AttType))
	}
	predataFile.MustPrintln(strings.Join(atts, ",\n"))
	predataFile.MustPrintf(");")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "TYPE")
	toc.AddPredataEntry(composite.TypeSchema, composite.TypeName, "TYPE", start, predataFile.ByteCount)
}

func PrintCreateEnumTypeStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, types []Type, typeMetadata MetadataMap) {
	start := predataFile.ByteCount
	for _, typ := range types {
		if typ.Type == "e" {
			typeFQN := MakeFQN(typ.TypeSchema, typ.TypeName)
			predataFile.MustPrintf("\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, typ.EnumLabels)
			PrintObjectMetadata(predataFile, typeMetadata[typ.Oid], typeFQN, "TYPE")
			toc.AddPredataEntry(typ.TypeSchema, typ.TypeName, "TYPE", start, predataFile.ByteCount)
		}
	}
}

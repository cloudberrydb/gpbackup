package backup

/*
 * This file contains structs and functions related to backing up type
 * metadata on the master that needs to be restored before data is restored.
 */

import (
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
			typeFQN := utils.MakeFQN(typ.Schema, typ.Name)
			predataFile.MustPrintf("CREATE TYPE %s;\n", typeFQN)
			toc.AddMetadataEntry(typ.Schema, typ.Name, "TYPE", start, predataFile)
			start = predataFile.ByteCount
		}
	}
}

func PrintCreateDomainStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, domain Type, typeMetadata ObjectMetadata, constraints []Constraint) {
	start := predataFile.ByteCount
	typeFQN := utils.MakeFQN(domain.Schema, domain.Name)
	predataFile.MustPrintf("\nCREATE DOMAIN %s AS %s", typeFQN, domain.BaseType)
	if domain.DefaultVal != "" {
		predataFile.MustPrintf(" DEFAULT %s", domain.DefaultVal)
	}
	if domain.NotNull {
		predataFile.MustPrintf(" NOT NULL")
	}
	for _, constraint := range constraints {
		predataFile.MustPrintf("\n\tCONSTRAINT %s %s", constraint.Name, constraint.ConDef)
	}
	predataFile.MustPrintln(";")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "DOMAIN")
	toc.AddMetadataEntry(domain.Schema, domain.Name, "DOMAIN", start, predataFile)
}

func PrintCreateBaseTypeStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, base Type, typeMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	typeFQN := utils.MakeFQN(base.Schema, base.Name)
	predataFile.MustPrintf("\n\nCREATE TYPE %s (\n", typeFQN)

	// All of the following functions are stored in quoted form and don't need to be quoted again
	predataFile.MustPrintf("\tINPUT = %s,\n\tOUTPUT = %s", base.Input, base.Output)
	if base.Receive != "" {
		predataFile.MustPrintf(",\n\tRECEIVE = %s", base.Receive)
	}
	if base.Send != "" {
		predataFile.MustPrintf(",\n\tSEND = %s", base.Send)
	}
	if connection.Version.AtLeast("5") {
		if base.ModIn != "" {
			predataFile.MustPrintf(",\n\tTYPMOD_IN = %s", base.ModIn)
		}
		if base.ModOut != "" {
			predataFile.MustPrintf(",\n\tTYPMOD_OUT = %s", base.ModOut)
		}
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
	toc.AddMetadataEntry(base.Schema, base.Name, "TYPE", start, predataFile)
}

func PrintCreateCompositeTypeStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, composite Type, typeMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	typeFQN := utils.MakeFQN(composite.Schema, composite.Name)
	predataFile.MustPrintf("\n\nCREATE TYPE %s AS (\n", typeFQN)
	predataFile.MustPrintln(strings.Join(composite.Attributes, ",\n"))
	predataFile.MustPrintf(");")
	PrintObjectMetadata(predataFile, typeMetadata, typeFQN, "TYPE")
	toc.AddMetadataEntry(composite.Schema, composite.Name, "TYPE", start, predataFile)
}

func PrintCreateEnumTypeStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, enums []Type, typeMetadata MetadataMap) {
	start := predataFile.ByteCount
	for _, enum := range enums {
		typeFQN := utils.MakeFQN(enum.Schema, enum.Name)
		predataFile.MustPrintf("\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, enum.EnumLabels)
		PrintObjectMetadata(predataFile, typeMetadata[enum.Oid], typeFQN, "TYPE")
		toc.AddMetadataEntry(enum.Schema, enum.Name, "TYPE", start, predataFile)
	}
}

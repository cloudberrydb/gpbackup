package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects relating to built-in text search that needs to be restored
 * before data is restored.
 *
 * Text search is not supported in GPDB 4.3, so none of these structs or functions
 * are used in a 4.3 backup.
 */

import (
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateTextSearchParserStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, parsers []TextSearchParser, parserMetadata MetadataMap) {
	for _, parser := range parsers {
		start := predataFile.ByteCount
		parserFQN := utils.MakeFQN(parser.Schema, parser.Name)
		predataFile.MustPrintf("\n\nCREATE TEXT SEARCH PARSER %s (", parserFQN)
		predataFile.MustPrintf("\n\tSTART = %s,", parser.StartFunc)
		predataFile.MustPrintf("\n\tGETTOKEN = %s,", parser.TokenFunc)
		predataFile.MustPrintf("\n\tEND = %s,", parser.EndFunc)
		predataFile.MustPrintf("\n\tLEXTYPES = %s", parser.LexTypesFunc)
		if parser.HeadlineFunc != "" {
			predataFile.MustPrintf(",\n\tHEADLINE = %s", parser.HeadlineFunc)
		}
		predataFile.MustPrintf("\n);")
		PrintObjectMetadata(predataFile, parserMetadata[parser.Oid], parserFQN, "TEXT SEARCH PARSER")
		toc.AddPredataEntry(parser.Schema, parser.Name, "TEXT SEARCH PARSER", start, predataFile.ByteCount)
	}
}

func PrintCreateTextSearchTemplateStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, templates []TextSearchTemplate, templateMetadata MetadataMap) {
	for _, template := range templates {
		start := predataFile.ByteCount
		templateFQN := utils.MakeFQN(template.Schema, template.Name)
		predataFile.MustPrintf("\n\nCREATE TEXT SEARCH TEMPLATE %s (", templateFQN)
		if template.InitFunc != "" {
			predataFile.MustPrintf("\n\tINIT = %s,", template.InitFunc)
		}
		predataFile.MustPrintf("\n\tLEXIZE = %s", template.LexizeFunc)
		predataFile.MustPrintf("\n);")
		PrintObjectMetadata(predataFile, templateMetadata[template.Oid], templateFQN, "TEXT SEARCH TEMPLATE")
		toc.AddPredataEntry(template.Schema, template.Name, "TEXT SEARCH TEMPLATE", start, predataFile.ByteCount)
	}
}

func PrintCreateTextSearchDictionaryStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, dictionaries []TextSearchDictionary, dictionaryMetadata MetadataMap) {
	for _, dictionary := range dictionaries {
		dictionaryFQN := utils.MakeFQN(dictionary.Schema, dictionary.Name)
		start := predataFile.ByteCount
		predataFile.MustPrintf("\n\nCREATE TEXT SEARCH DICTIONARY %s (", dictionaryFQN)
		predataFile.MustPrintf("\n\tTEMPLATE = %s", dictionary.Template)
		if dictionary.InitOption != "" {
			predataFile.MustPrintf(",\n\t%s", dictionary.InitOption)
		}
		predataFile.MustPrintf("\n);")
		PrintObjectMetadata(predataFile, dictionaryMetadata[dictionary.Oid], dictionaryFQN, "TEXT SEARCH DICTIONARY")
		toc.AddPredataEntry(dictionary.Schema, dictionary.Name, "TEXT SEARCH DICTIONARY", start, predataFile.ByteCount)
	}
}

func PrintCreateTextSearchConfigurationStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, configurations []TextSearchConfiguration, configurationMetadata MetadataMap) {
	for _, configuration := range configurations {
		configurationFQN := utils.MakeFQN(configuration.Schema, configuration.Name)
		start := predataFile.ByteCount
		predataFile.MustPrintf("\n\nCREATE TEXT SEARCH CONFIGURATION %s (", configurationFQN)
		predataFile.MustPrintf("\n\tPARSER = %s", configuration.Parser)
		predataFile.MustPrintf("\n);")
		tokens := []string{}
		for token := range configuration.TokenToDicts {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			dicts := configuration.TokenToDicts[token]
			predataFile.MustPrintf("\n\nALTER TEXT SEARCH CONFIGURATION %s", configurationFQN)
			predataFile.MustPrintf("\n\tADD MAPPING FOR \"%s\" WITH %s;", token, strings.Join(dicts, ", "))
		}
		PrintObjectMetadata(predataFile, configurationMetadata[configuration.Oid], configurationFQN, "TEXT SEARCH CONFIGURATION")
		toc.AddPredataEntry(configuration.Schema, configuration.Name, "TEXT SEARCH CONFIGURATION", start, predataFile.ByteCount)
	}
}

package backup

/*
 * This file contains structs and functions related to dumping metadata on the
 * master for objects relating to built-in text search that needs to be restored
 * before data is restored.
 */

import (
	"io"
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateTextSearchParserStatements(predataFile io.Writer, parsers []TextSearchParser, parserMetadata MetadataMap) {
	for _, parser := range parsers {
		parserFQN := MakeFQN(parser.Schema, parser.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE TEXT SEARCH PARSER %s (", parserFQN)
		utils.MustPrintf(predataFile, "\n\tSTART = %s,", parser.StartFunc)
		utils.MustPrintf(predataFile, "\n\tGETTOKEN = %s,", parser.TokenFunc)
		utils.MustPrintf(predataFile, "\n\tEND = %s,", parser.EndFunc)
		utils.MustPrintf(predataFile, "\n\tLEXTYPES = %s", parser.LexTypesFunc)
		if parser.HeadlineFunc != "" {
			utils.MustPrintf(predataFile, ",\n\tHEADLINE = %s", parser.HeadlineFunc)
		}
		utils.MustPrintf(predataFile, "\n);")
		PrintObjectMetadata(predataFile, parserMetadata[parser.Oid], parserFQN, "TEXT SEARCH PARSER")
	}
}

func PrintCreateTextSearchTemplateStatements(predataFile io.Writer, templates []TextSearchTemplate, templateMetadata MetadataMap) {
	for _, template := range templates {
		templateFQN := MakeFQN(template.Schema, template.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE TEXT SEARCH TEMPLATE %s (", templateFQN)
		if template.InitFunc != "" {
			utils.MustPrintf(predataFile, "\n\tINIT = %s,", template.InitFunc)
		}
		utils.MustPrintf(predataFile, "\n\tLEXIZE = %s", template.LexizeFunc)
		utils.MustPrintf(predataFile, "\n);")
		PrintObjectMetadata(predataFile, templateMetadata[template.Oid], templateFQN, "TEXT SEARCH TEMPLATE")
	}
}

func PrintCreateTextSearchDictionaryStatements(predataFile io.Writer, dictionaries []TextSearchDictionary, dictionaryMetadata MetadataMap) {
	for _, dictionary := range dictionaries {
		dictionaryFQN := MakeFQN(dictionary.Schema, dictionary.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE TEXT SEARCH DICTIONARY %s (", dictionaryFQN)
		utils.MustPrintf(predataFile, "\n\tTEMPLATE = %s", dictionary.Template)
		if dictionary.InitOption != "" {
			utils.MustPrintf(predataFile, ",\n\t%s", dictionary.InitOption)
		}
		utils.MustPrintf(predataFile, "\n);")
		PrintObjectMetadata(predataFile, dictionaryMetadata[dictionary.Oid], dictionaryFQN, "TEXT SEARCH DICTIONARY")
	}
}

func PrintCreateTextSearchConfigurationStatements(predataFile io.Writer, configurations []TextSearchConfiguration, configurationMetadata MetadataMap) {
	for _, configuration := range configurations {
		configurationFQN := MakeFQN(configuration.Schema, configuration.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE TEXT SEARCH CONFIGURATION %s (", configurationFQN)
		utils.MustPrintf(predataFile, "\n\tPARSER = %s", configuration.Parser)
		utils.MustPrintf(predataFile, "\n);")
		tokens := []string{}
		for token := range configuration.TokenToDicts {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			dicts := configuration.TokenToDicts[token]
			utils.MustPrintf(predataFile, "\n\nALTER TEXT SEARCH CONFIGURATION %s", configurationFQN)
			utils.MustPrintf(predataFile, "\n\tADD MAPPING FOR \"%s\" WITH %s;", token, strings.Join(dicts, ", "))
		}
		PrintObjectMetadata(predataFile, configurationMetadata[configuration.Oid], configurationFQN, "TEXT SEARCH CONFIGURATION")
	}
}

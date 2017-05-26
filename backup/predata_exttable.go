package backup

/*
 * This file contains structs and functions related to dumping external table
 * metadata on the master.
 */

import (
	"fmt"
	"gpbackup/utils"
	"io"
	"strings"
)

const (
	// Type of external table
	READABLE = iota
	READABLE_WEB
	WRITABLE
	WRITABLE_WEB
	// Protocol external table is using
	FILE
	GPFDIST
	GPHDFS
	HTTP
	S3
)

type ExternalTableDefinition struct {
	Type            int
	Protocol        int
	Location        string
	ExecLocation    string
	FormatType      string
	FormatOpts      string
	Options         string
	Command         string
	RejectLimit     int
	RejectLimitType string
	ErrTable        string
	Encoding        string
	Writable        bool
}

func PrintExternalTableCreateStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	tableTypeStrMap := map[int]string{
		READABLE:     "READABLE EXTERNAL",
		READABLE_WEB: "READABLE EXTERNAL WEB",
		WRITABLE:     "WRITABLE EXTERNAL",
		WRITABLE_WEB: "WRITABLE EXTERNAL WEB",
	}
	extTableDef := tableDef.ExtTableDef
	extTableDef.Type, extTableDef.Protocol = DetermineExternalTableCharacteristics(extTableDef)
	fmt.Fprintf(predataFile, "\n\nCREATE %s TABLE %s (\n", tableTypeStrMap[extTableDef.Type], table.ToString())
	printColumnStatements(predataFile, table, tableDef.ColumnDefs)
	fmt.Fprintf(predataFile, ") ")
	PrintExternalTableStatements(predataFile, table, extTableDef)
	fmt.Fprintf(predataFile, "%s;", tableDef.DistPolicy)
}

func DetermineExternalTableCharacteristics(extTableDef ExternalTableDefinition) (int, int) {
	isWritable := extTableDef.Writable
	tableType := -1
	tableProtocol := -1
	if extTableDef.Location == "" { // EXTERNAL WEB tables may have EXECUTE instead of LOCATION
		tableProtocol = HTTP
		if isWritable {
			tableType = WRITABLE_WEB
		} else {
			tableType = READABLE_WEB
		}
	} else {
		isWeb := strings.HasPrefix(extTableDef.Location, "http")
		if isWeb && isWritable {
			tableType = WRITABLE_WEB
		} else if isWeb && !isWritable {
			tableType = READABLE_WEB
		} else if !isWeb && isWritable {
			tableType = WRITABLE
		} else {
			tableType = READABLE
		}
		prefix := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
		switch prefix {
		case "file":
			tableProtocol = FILE
		case "gpfdist":
			tableProtocol = GPFDIST
		case "gpfdists":
			tableProtocol = GPFDIST
		case "gphdfs":
			tableProtocol = GPHDFS
		case "http":
			tableProtocol = HTTP
		case "https":
			tableProtocol = HTTP
		case "s3":
			tableProtocol = S3
		}
	}
	return tableType, tableProtocol
}

func PrintExternalTableStatements(predataFile io.Writer, table utils.Relation, extTableDef ExternalTableDefinition) {
	if extTableDef.Type != WRITABLE_WEB {
		if extTableDef.Location != "" {
			locations := make([]string, 0)
			for _, loc := range strings.Split(extTableDef.Location, ",") {
				locations = append(locations, fmt.Sprintf("\t'%s'", loc))
			}
			fmt.Fprintf(predataFile, "LOCATION (\n%s\n)", strings.Join(locations, "\n"))
		}
	}
	if extTableDef.Type == READABLE || (extTableDef.Type == WRITABLE_WEB && extTableDef.Protocol == S3) {
		if extTableDef.ExecLocation == "MASTER_ONLY" {
			fmt.Fprintf(predataFile, " ON MASTER")
		}
	}
	if extTableDef.Type == READABLE_WEB || extTableDef.Type == WRITABLE_WEB {
		if extTableDef.Command != "" {
			fmt.Fprintf(predataFile, "EXECUTE '%s'", extTableDef.Command)
			execType := strings.Split(extTableDef.ExecLocation, ":")
			switch execType[0] {
			case "ALL_SEGMENTS": // Default case, don't print anything else
			case "HOST":
				fmt.Fprintf(predataFile, " ON HOST '%s'", execType[1])
			case "MASTER_ONLY":
				fmt.Fprintf(predataFile, " ON MASTER")
			case "PER_HOST":
				fmt.Fprintf(predataFile, " ON HOST")
			case "SEGMENT_ID":
				fmt.Fprintf(predataFile, " ON SEGMENT %s", execType[1])
			case "TOTAL_SEGS":
				fmt.Fprintf(predataFile, " ON %s", execType[1])
			}
		}
	}
	fmt.Fprintln(predataFile)
	formatType := ""
	switch extTableDef.FormatType {
	case "a":
		formatType = "avro"
	case "b":
		formatType = "custom"
	case "c":
		formatType = "csv"
	case "p":
		formatType = "parquet"
	case "t":
		formatType = "text"
	}
	/*
	 * The options for the custom formatter is stored as "formatter 'function_name'",
	 * but FORMAT requires "formatter='function_name'".
	 */
	extTableDef.FormatOpts = strings.Replace(extTableDef.FormatOpts, "formatter ", "formatter=", 1)
	fmt.Fprintf(predataFile, "FORMAT '%s'", formatType)
	if extTableDef.FormatOpts != "" {
		fmt.Fprintf(predataFile, " (%s)", strings.TrimSpace(extTableDef.FormatOpts))
	}
	fmt.Fprintln(predataFile)
	if extTableDef.Options != "" {
		fmt.Fprintf(predataFile, "OPTIONS (\n\t%s\n)\n", extTableDef.Options)
	}
	fmt.Fprintf(predataFile, "ENCODING '%s'\n", extTableDef.Encoding)
	if extTableDef.Type == READABLE || extTableDef.Type == READABLE_WEB {
		/*
		 * In GPDB 5 and later, LOG ERRORS INTO [table] has been replaced by LOG ERRORS,
		 * but it still uses the same catalog entries to store that information.  If the
		 * value of pg_exttable.fmterrtbl matches the table's own name, LOG ERRORS is set.
		 */
		if extTableDef.ErrTable == table.RelationName {
			fmt.Fprintln(predataFile, "LOG ERRORS")
		}
		if extTableDef.RejectLimit != 0 {
			fmt.Fprintf(predataFile, "SEGMENT REJECT LIMIT %d ", extTableDef.RejectLimit)
			switch extTableDef.RejectLimitType {
			case "r":
				fmt.Fprintln(predataFile, "ROWS")
			case "p":
				fmt.Fprintln(predataFile, "PERCENT")
			}
		}
	}
}

package options

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/pflag"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

type Options struct {
	includedRelations []string
}

func (o Options) GetIncludedTables() []string {
	return o.includedRelations
}

func NewOptions(initialFlags *pflag.FlagSet) (*Options, error) {
	tableNames, err := initialFlags.GetStringArray(utils.INCLUDE_RELATION)
	if err != nil {
		return nil, err
	}

	// flag for INCLUDE_RELATION_FILE is mutually exclusive with INCLUDE_RELATION flag
	// so this is not an overwrite, we are just skipping testing if either was set
	filename, err := initialFlags.GetString(utils.INCLUDE_RELATION_FILE)
	if err != nil {
		return nil, err
	}
	if filename != "" {
		tableNames = iohelper.MustReadLinesFromFile(filename)
	}

	err = ValidateCharacters(tableNames)
	if err != nil {
		return nil, err
	}

	return &Options{
		includedRelations: tableNames,
	}, nil
}

type FqnStruct struct {
	SchemaName string
	TableName  string
}

const QuoteIdent = `SELECT quote_ident('%s') AS schemaname, quote_ident('%s') AS tablename`

func QuoteTableNames(conn *dbconn.DBConn, tableNames []string) ([]string, error) {
	fqnSlice, err := SeparateSchemaAndTable(tableNames)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	for _, fqn := range fqnSlice {
		queryResultTable := make([]FqnStruct, 0)
		query := fmt.Sprintf(QuoteIdent, fqn.SchemaName, fqn.TableName)
		err := conn.Select(&queryResultTable, query)
		if err != nil {
			return nil, err
		}
		quoted := queryResultTable[0].SchemaName + "." + queryResultTable[0].TableName
		result = append(result, quoted)
	}

	return result, nil
}

func SeparateSchemaAndTable(tableNames []string) ([]FqnStruct, error) {
	fqnSlice := make([]FqnStruct, 0)
	for _, fqn := range tableNames {
		parts := strings.Split(fqn, ".")
		if len(parts) > 2 {
			return nil, errors.Errorf("cannot process an Fully Qualified Name with embedded dots yet: %s", fqn)
		}
		if len(parts) < 2 {
			return nil, errors.Errorf("Fully Qualified Names require a minimum of one dot, specifying the schema and table. Cannot process: %s", fqn)
		}
		schema := parts[0]
		table := parts[1]
		if schema == "" || table == "" {
			return nil, errors.Errorf("Fully Qualified Names must specify the schema and table. Cannot process: %s", fqn)
		}

		currFqn := FqnStruct{
			SchemaName: schema,
			TableName:  table,
		}

		fqnSlice = append(fqnSlice, currFqn)
	}

	return fqnSlice, nil
}

func ValidateCharacters(tableList []string) error {
	if len(tableList) == 0 {
		return nil
	}

	validFormat := regexp.MustCompile(`^.+\..+$`)
	for _, fqn := range tableList {
		if !validFormat.Match([]byte(fqn)) {
			return errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn)
		}
	}

	return nil
}

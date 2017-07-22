package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		testutils.SetupTestLogger()
	})
	Describe("GetAllUserSchemas", func() {
		It("returns user schema information", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connection)

			schemaBar := utils.Schema{0, "bar"}
			schemaPublic := utils.Schema{2200, "public"}

			Expect(len(schemas)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")
		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			testutils.AssertQueryRuns(connection, "CREATE LANGUAGE plperl")
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plperl")

			pgsqlHandlerOid := backup.OidFromObjectName(connection, "plpgsql_call_handler", backup.FunctionParams)
			pgsqlInlineOid := backup.OidFromObjectName(connection, "plpgsql_inline_handler", backup.FunctionParams)
			pgsqlValidatorOid := backup.OidFromObjectName(connection, "plpgsql_validator", backup.FunctionParams)

			perlHandlerOid := backup.OidFromObjectName(connection, "plperl_call_handler", backup.FunctionParams)
			perlInlineOid := backup.OidFromObjectName(connection, "plperl_inline_handler", backup.FunctionParams)
			perlValidatorOid := backup.OidFromObjectName(connection, "plperl_validator", backup.FunctionParams)

			expectedPlpgsqlInfo := backup.QueryProceduralLanguage{0, "plpgsql", "testrole", true, true, pgsqlHandlerOid, pgsqlInlineOid, pgsqlValidatorOid}
			expectedPlperlInfo := backup.QueryProceduralLanguage{1, "plperl", "testrole", true, true, perlHandlerOid, perlInlineOid, perlValidatorOid}

			resultProcLangs := backup.GetProceduralLanguages(connection)

			Expect(len(resultProcLangs)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&expectedPlpgsqlInfo, &resultProcLangs[0], "Oid", "Owner")
			testutils.ExpectStructsToMatchExcluding(&expectedPlperlInfo, &resultProcLangs[1], "Oid", "Owner")
		})
	})
})

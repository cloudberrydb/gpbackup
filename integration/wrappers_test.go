package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
)

var _ = Describe("Wrappers Integration", func() {
	Describe("RetrieveAndProcessTables", func() {
		It("returns the single data table that has data to be backed up", func() {
			var rootCmd = &cobra.Command{}
			backup.DoInit(rootCmd)             // initialize the ObjectCount
			gplog.SetVerbosity(gplog.LOGERROR) // turn off progress bar in the lock-table routine
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int); INSERT INTO public.foo VALUES (1);")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public."BAR"(i int); INSERT INTO public."BAR" VALUES (1);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo;")
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public."BAR";`)

			// every backup occurs in a transaction; we are testing a small part of that backup
			connectionPool.MustBegin(0)
			defer connectionPool.MustCommit(0)

			_ = backupCmdFlags.Set(utils.INCLUDE_RELATION, "public.foo")
			_ = backupCmdFlags.Set(utils.INCLUDE_RELATION, "public.BAR")

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
		})
	})

})
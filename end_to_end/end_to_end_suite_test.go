package end_to_end_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup(gpbackupPath string, args ...string) string {
	args = append([]string{"-dbname", "testdb"}, args...)
	command := exec.Command(gpbackupPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	r := regexp.MustCompile(`Backup Timestamp = (\d{14})`)
	return r.FindStringSubmatch(fmt.Sprintf("%s", output))[1]
}

func gprestore(gprestorePath string, timestamp string, args ...string) []byte {
	args = append([]string{"-timestamp", timestamp}, args...)
	command := exec.Command(gprestorePath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
}

func buildAndInstallBinaries() (string, string) {
	os.Chdir("..")
	command := exec.Command("make", "build")
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	os.Chdir("end_to_end")
	binDir := fmt.Sprintf("%s/go/bin", utils.System.Getenv("HOME"))
	return fmt.Sprintf("%s/gpbackup", binDir), fmt.Sprintf("%s/gprestore", binDir)
}

func assertDataRestored(conn *utils.DBConn, tableToTupleCount map[string]int) {
	for name, numTuples := range tableToTupleCount {
		tupleCount := utils.SelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", name))
		Expect(tupleCount).To(Equal(strconv.Itoa(numTuples)))
	}
}

func assertTablesCreated(conn *utils.DBConn, numTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_tables WHERE schemaname IN ('public','schema2')`
	tableCount := utils.SelectString(conn, countQuery)
	Expect(tableCount).To(Equal(strconv.Itoa(numTables)))
}

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = Describe("backup end to end integration tests", func() {

	var backupConn, restoreConn *utils.DBConn
	var gpbackupPath, gprestorePath string
	BeforeSuite(func() {
		var err error
		testutils.SetupTestLogger()
		exec.Command("dropdb", "testdb").Run()
		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create testdb: %v", err))
		}
		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create restoredb: %v", err))
		}
		backupConn = utils.NewDBConn("testdb")
		backupConn.Connect(1)
		restoreConn = utils.NewDBConn("restoredb")
		restoreConn.Connect(1)
		testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
		gpbackupPath, gprestorePath = buildAndInstallBinaries()
	})
	AfterSuite(func() {
		if backupConn != nil {
			backupConn.Close()
		}
		if restoreConn != nil {
			restoreConn.Close()
		}
		gexec.CleanupBuildArtifacts()
		err := exec.Command("dropdb", "testdb").Run()
		if err != nil {
			fmt.Printf("Could not drop testdb: %v\n", err)
		}
		err = exec.Command("dropdb", "restoredb").Run()
		if err != nil {
			fmt.Printf("Could not drop restoredb: %v\n", err)
		}
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		publicSchemaTupleCounts := map[string]int{"public.foo": 40000, "public.holds": 50000, "public.sales": 13}
		schema2TupleCounts := map[string]int{"schema2.returns": 6, "schema2.foo2": 0, "schema2.foo3": 100}
		BeforeEach(func() {
			testutils.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		})
		It("runs gpbackup and gprestore without redirecting restore to another db", func() {
			timestamp := gpbackup(gpbackupPath)
			backupConn.Close()
			err := exec.Command("dropdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			gprestore(gprestorePath, timestamp, "-createdb")
			backupConn = utils.NewDBConn("testdb")
			backupConn.Connect(1)
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, "-metadata-only")
			timestamp2 := gpbackup(gpbackupPath, "-data-only")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertTablesCreated(restoreConn, 30)
			gprestore(gprestorePath, timestamp2, "-redirect", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with include-schema flag and compression level", func() {
			timestamp := gpbackup(gpbackupPath, "-include-schema", "public", "-compression-level", "2")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-table flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-table", "schema2.foo2", "-exclude-table", "schema2.foo3", "-exclude-table", "schema2.returns")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, map[string]int{"public.foo": 40000, "public.holds": 50000, "public.sales": 13})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table flag", func() {
			timestamp := gpbackup(gpbackupPath, "-include-table", "public.foo", "-include-table", "public.sales")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with exclude-table-file flag", func() {
			excludeFile := utils.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.foo3\nschema2.returns\npublic.sales")
			timestamp := gpbackup(gpbackupPath, "-exclude-table-file", "/tmp/exclude-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 2)
			assertDataRestored(restoreConn, map[string]int{"public.foo": 40000, "public.holds": 50000})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table-file flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			timestamp := gpbackup(gpbackupPath, "-include-table-file", "/tmp/include-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := "/tmp/leaf_partition_data"
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data", "-backupdir", backupdir)
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			Expect(strings.Contains(string(output), "Tables restored:  28 / 28")).To(BeTrue())

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := "/tmp/no_compression"
			timestamp := gpbackup(gpbackupPath, "-no-compression", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			configFile, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*config.yaml"))
			contents, _ := ioutil.ReadFile(configFile[0])

			Expect(strings.Contains(string(contents), "compressed: false")).To(BeTrue())
			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			backupdir := "/tmp/with_stats"
			timestamp := gpbackup(gpbackupPath, "-with-stats", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))

			Expect(len(files)).To(Equal(1))
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-with-stats", "-backupdir", backupdir)

			Expect(strings.Contains(string(output), "Query planner statistics restore complete")).To(BeTrue())
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with single-data-file flag", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backupdir", backupdir, "-no-compression")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with include-table-file restore flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-table-file", "/tmp/include-tables.txt")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-table-file", "/tmp/include-tables.txt")
			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-schema restore flag", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with jobs flag", func() {
			backupdir := "/tmp/parallel"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-jobs", "4")

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			testutils.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			/* We do not check the error code since there are some objects we
			 * expect to fail during creation between DB versions
			 */
			exec.Command("psql", "-d", "testdb", "-f", "all_objects.sql", "-q")
			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			testutils.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
		})
		It("runs gpbackup and sends a SIGINT to ensure cleanup functions successfully", func() {
			backupdir := "/tmp/signals"
			args := []string{"-dbname", "testdb", "-backupdir", backupdir, "-single-data-file", "-verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between
				 * 0.5s and 1.5s) so that gpbackup will be interrupted at a
				 * different point in the backup process every time to help
				 * catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))

			os.RemoveAll(backupdir)
		})
		It("runs gprestore and sends a SIGINT to ensure cleanup functions successfully", func() {
			backupdir := "/tmp/signals"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir, "-single-data-file")
			args := []string{"-timestamp", timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-schema", "schema2", "-verbose"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between
				 * 0.5s and 1.5s) so that gprestore will be interrupted at a
				 * different point in the backup process every time to help
				 * catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))

			os.RemoveAll(backupdir)
		})
	})
})

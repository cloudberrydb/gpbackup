package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/greenplum-db/gpbackup/backup"
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

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = Describe("backup end to end integration tests", func() {

	var gpbackupPath, gprestorePath string
	var backupConn, restoreConn *utils.DBConn
	BeforeSuite(func() {
		var err error
		gpbackupPath, err = gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup", "-ldflags", "-X github.com/greenplum-db/gpbackup/backup.version=0.5.0")
		Expect(err).ShouldNot(HaveOccurred())
		gprestorePath, err = gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gprestore", "-ldflags", "-X github.com/greenplum-db/gpbackup/restore.version=0.5.0")
		Expect(err).ShouldNot(HaveOccurred())
		exec.Command("dropdb", "testdb").Run()
		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
		backupConn = utils.NewDBConn("testdb")
		backupConn.Connect()
		utils.ExecuteSQLFile(backupConn, "test_tables.sql")
	})
	AfterSuite(func() {
		backupConn.Close()
		restoreConn.Close()
		gexec.CleanupBuildArtifacts()
		exec.Command("dropdb", "testdb").Run()
		exec.Command("dropdb", "restoredb").Run()
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		countQuery := `SELECT count(*) AS string FROM pg_tables WHERE schemaname IN ('public','schema2')`
		BeforeEach(func() {
			if restoreConn != nil {
				restoreConn.Close()
			}
			exec.Command("dropdb", "restoredb").Run()
			err := exec.Command("createdb", "restoredb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			restoreConn = utils.NewDBConn("restoredb")
			restoreConn.Connect()
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
			backupConn.Connect()
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, "-metadata-only")
			timestamp2 := gpbackup(gpbackupPath, "-data-only")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			gprestore(gprestorePath, timestamp2, "-redirect", "restoredb")
		})
		It("runs gpbackup and gprestore with exclude-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("14"))
		})
		It("runs gpbackup and gprestore with include-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-include-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("15"))
		})
		It("runs gpbackup and gprestore with exclude-table-file flag", func() {
			excludeFile := utils.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.returns\npublic.sales")
			timestamp := gpbackup(gpbackupPath, "-exclude-table-file", "/tmp/exclude-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("2"))
			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table-file flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales")
			timestamp := gpbackup(gpbackupPath, "-include-table-file", "/tmp/include-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("13"))
			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := "/tmp/leaf_partition_data"
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*0/backups/*", timestamp, "*.gz"))
			Expect(len(files)).To(Equal(27))
			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := "/tmp/no_compression"
			timestamp := gpbackup(gpbackupPath, "-no-compression", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*0/backups/*", timestamp, "*"))
			gzfiles, _ := filepath.Glob(filepath.Join(backupdir, "*0/backups/*", timestamp, "*.gz"))
			Expect(len(files)).To(Equal(5))
			Expect(len(gzfiles)).To(Equal(0))
			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			backupdir := "/tmp/with_stats"
			timestamp := gpbackup(gpbackupPath, "-with-stats", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))
			Expect(len(files)).To(Equal(1))
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-with-stats", "-backupdir", backupdir)
			r := regexp.MustCompile(`Query planner statistics restore complete`)
			Expect(r.Match(output)).To(BeTrue())
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			backupConn.Close()
			exec.Command("dropdb", "testdb").Run()
			err := exec.Command("createdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			backupConn = utils.NewDBConn("testdb")
			backupConn.Connect()
			utils.ExecuteSQLFile(backupConn, "all_objects.sql")

			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			backupConn.Close()
			exec.Command("dropdb", "testdb").Run()
			err = exec.Command("createdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			backupConn = utils.NewDBConn("testdb")
			backupConn.Connect()
			utils.ExecuteSQLFile(backupConn, "test_tables.sql")
		})
	})
})

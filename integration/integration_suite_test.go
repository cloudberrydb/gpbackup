package integration

import (
	"testing"

	"os/exec"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	connection *utils.DBConn
)

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "database query tests")
}

var _ = BeforeSuite(func() {
	exec.Command("dropdb", "testdb").Run()
	err := exec.Command("createdb", "testdb").Run()
	if err != nil {
		Fail("Cannot create database testdb; is GPDB running?")
	}
	Expect(err).To(BeNil())
	connection = utils.NewDBConn("testdb")
	connection.Connect()
	// We can't use AssertQueryRuns since if a role already exists it will error
	connection.Exec("CREATE ROLE testrole SUPERUSER")
	connection.Exec("CREATE ROLE gpadmin SUPERUSER")
	testutils.AssertQueryRuns(connection, "SET ROLE testrole")
	testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb OWNER TO gpadmin")
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
	if connection != nil {
		connection.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
})

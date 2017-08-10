package utils_test

import (
	"testing"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	logger  *utils.Logger
	stdout  *gbytes.Buffer
	stderr  *gbytes.Buffer
	logfile *gbytes.Buffer
	buffer  *gbytes.Buffer
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "utils tests")
}

var _ = BeforeSuite(func() {
	logger, stdout, stderr, logfile = testutils.SetupTestLogger()
	utils.System = utils.InitializeSystemFunctions()
})

var _ = BeforeEach(func() {
	logger, stdout, stderr, logfile = testutils.SetupTestLogger()
	utils.System = utils.InitializeSystemFunctions()
	buffer = gbytes.NewBuffer()
})

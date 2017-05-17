package backup_test

/*
 * This file contains integration tests for gpbackup as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var gpbackupPath = ""

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup() *gexec.Session {
	command := exec.Command(gpbackupPath, "-dbname", "testdb")
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	<-session.Exited
	return session
}

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "backup tests")
}

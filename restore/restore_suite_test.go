package restore_test

/*
 * This file contains integration tests for gprestore as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var gprestorePath = ""

/* This function is a helper function to execute gprestore and return a session
 * to allow checking its output.
 */
func gprestore() *gexec.Session {
	command := exec.Command(gprestorePath, "-timestamp", "20170101010101")
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	<-session.Exited
	return session
}

func TestRestore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "restore tests")
}

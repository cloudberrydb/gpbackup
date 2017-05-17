package utils_test

import (
	"gpbackup/utils"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/util tests", func() {
	Context("CurrentTimestamp", func() {
		It("returns the current timestamp", func() {
			utils.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
			expected := "20170101010101"
			actual := utils.CurrentTimestamp()
			Expect(actual).To(Equal(expected))
		})
	})
})

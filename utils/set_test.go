package utils_test

import (
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/set tests", func() {
	var inc, exc, incEmpty, excEmpty *utils.FilterSet
	BeforeEach(func() {
		inc = utils.NewIncludeSet([]string{"foo", "bar"})
		exc = utils.NewExcludeSet([]string{"foo", "bar"})
		incEmpty = utils.NewIncludeSet([]string{})
		excEmpty = utils.NewExcludeSet([]string{})
	})
	Describe("NewIncludeSet", func() {
		It("can create a set from an empty list", func() {
			expectedMap := map[string]bool{}
			Expect(incEmpty.Set).To(Equal(expectedMap))
			Expect(incEmpty.IsExclude).To(BeFalse())
			Expect(incEmpty.AlwaysMatchesFilter).To(BeTrue())
		})
		It("can create a set from a list of strings", func() {
			expectedMap := map[string]bool{"foo": true, "bar": true}
			Expect(inc.Set).To(Equal(expectedMap))
			Expect(inc.IsExclude).To(BeFalse())
			Expect(inc.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("NewExcludeSet", func() {
		It("can create a set from an empty list", func() {
			expectedMap := map[string]bool{}
			Expect(excEmpty.Set).To(Equal(expectedMap))
			Expect(excEmpty.IsExclude).To(BeTrue())
			Expect(excEmpty.AlwaysMatchesFilter).To(BeTrue())
		})
		It("can create a set from a list of strings", func() {
			expectedMap := map[string]bool{"foo": true, "bar": true}
			Expect(exc.Set).To(Equal(expectedMap))
			Expect(exc.IsExclude).To(BeTrue())
			Expect(exc.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("MatchesFilter", func() {
		It("returns true if an item is in a non-empty include set", func() {
			Expect(inc.MatchesFilter("foo")).To(BeTrue())
		})
		It("returns false if an item is not in a non-empty include set", func() {
			Expect(inc.MatchesFilter("nonexistent")).To(BeFalse())
		})
		It("returns true if an include set is empty", func() {
			Expect(incEmpty.MatchesFilter("foo")).To(BeTrue())
		})
		It("returns true if an item is not in a non-empty exclude set", func() {
			Expect(exc.MatchesFilter("nonexistent")).To(BeTrue())
		})
		It("returns false if an item is in a non-empty exclude set", func() {
			Expect(exc.MatchesFilter("foo")).To(BeFalse())
		})
		It("returns true if an exclude set is empty", func() {
			Expect(excEmpty.MatchesFilter("foo")).To(BeTrue())
		})
	})
	Describe("Length", func() {
		It("returns the length of the underlying map", func() {
			Expect(inc.Length()).To(Equal(2))
		})
	})
	Describe("Add", func() {
		It("adds an item to a non-empty set", func() {
			Expect(inc.Length()).To(Equal(2))
			Expect(inc.AlwaysMatchesFilter).To(BeFalse())
			inc.Add("baz")
			Expect(inc.Length()).To(Equal(3))
			Expect(inc.AlwaysMatchesFilter).To(BeFalse())
		})
		It("adds an item to an empty set and sets AlwaysMatchesFilter to false", func() {
			Expect(incEmpty.Length()).To(Equal(0))
			Expect(incEmpty.AlwaysMatchesFilter).To(BeTrue())
			incEmpty.Add("baz")
			Expect(incEmpty.Length()).To(Equal(1))
			Expect(incEmpty.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("Delete", func() {
		It("removes an item from a non-empty set and sets AlwaysMatchesFilter to true if the new length is 0", func() {
			Expect(inc.Length()).To(Equal(2))
			Expect(inc.AlwaysMatchesFilter).To(BeFalse())
			inc.Delete("bar")
			Expect(inc.Length()).To(Equal(1))
			Expect(inc.AlwaysMatchesFilter).To(BeFalse())
			inc.Delete("foo")
			Expect(inc.Length()).To(Equal(0))
			Expect(inc.AlwaysMatchesFilter).To(BeTrue())
		})
	})
})

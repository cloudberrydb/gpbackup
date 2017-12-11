package helper_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gpbackup/helper"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("helper/helper", func() {
	var stdinRead, stdinWrite *os.File
	var tocFileRead *os.File
	BeforeEach(func() {
		stdinRead, stdinWrite, _ = os.Pipe()
		tocFileRead, _, _ = os.Pipe()
		utils.System.Stdin = stdinRead
		stdout = gbytes.NewBuffer()
		utils.System.Stdout = stdout
	})
	AfterEach(func() {
		utils.System.OpenFileRead = utils.OpenFileRead
		utils.System.Stat = os.Stat
		utils.System.Stdin = os.Stdin
		utils.System.Stdout = os.Stdout
		utils.System.ReadFile = ioutil.ReadFile
	})
	Describe("ReadAndCountBytes", func() {
		It("Returns correct number of bytes read", func() {
			fmt.Fprintln(stdinWrite, "some text")
			stdinWrite.Close()
			bytesRead := helper.ReadAndCountBytes()
			Expect(bytesRead).To(Equal(uint64(10)))
			Expect(stdout).To(gbytes.Say("some text\n"))
		})
		It("Returns 0 if no bytes read", func() {
			stdinWrite.Close()
			bytesRead := helper.ReadAndCountBytes()
			Expect(bytesRead).To(Equal(uint64(0)))
			Expect(stdout).To(gbytes.Say(""))
		})
		Describe("ReadOrCreateTOC", func() {
			It("returns contents of TOC when a TOC file exists", func() {
				helper.SetFilename("filename")
				utils.System.Stat = func(name string) (os.FileInfo, error) {
					return nil, nil
				}
				utils.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (utils.ReadCloserAt, error) { return tocFileRead, nil }
				utils.System.ReadFile = func(filename string) ([]byte, error) {
					return []byte(`lastbyteread: 15
dataentries:
  1:
    startbyte: 0
    endbyte: 5
  2:
    startbyte: 5
    endbyte: 10
  3:
    startbyte: 10
    endbyte: 15`), nil
				}
				expectedDataEntries := map[uint]utils.SegmentDataEntry{
					1: {StartByte: 0, EndByte: 5},
					2: {StartByte: 5, EndByte: 10},
					3: {StartByte: 10, EndByte: 15},
				}
				toc, lastRead := helper.ReadOrCreateTOC()
				Expect(lastRead).To(Equal(uint64(15)))
				Expect((*toc).DataEntries).To(Equal(expectedDataEntries))
			})
			It("returns a new TOC when no TOC file exists", func() {
				helper.SetFilename("filename")
				utils.System.Stat = func(name string) (os.FileInfo, error) {
					return nil, os.ErrNotExist
				}
				toc, lastRead := helper.ReadOrCreateTOC()
				Expect(lastRead).To(Equal(uint64(0)))
				Expect((*toc).DataEntries).To(Not(BeNil()))
			})
		})
		Describe("GetBoundsForTable", func() {
			It("returns the start and end byte from the TOC", func() {
				helper.SetFilename("filename")
				toc := &utils.SegmentTOC{}
				toc.DataEntries = map[uint]utils.SegmentDataEntry{
					1: {StartByte: 0, EndByte: 5},
					2: {StartByte: 5, EndByte: 10},
					3: {StartByte: 10, EndByte: 15},
				}
				helper.SetOid(3)
				startByte, endByte := helper.GetBoundsForTable(toc)
				Expect(startByte).To(Equal(int64(10)))
				Expect(endByte).To(Equal(int64(15)))
			})
		})
		Describe("CopyByteRange", func() {
			BeforeEach(func() {
				helper.SetContent(1)
				helper.SetOid(3)
			})
			It("can copy a byte range without seeking", func() {
				fmt.Fprintln(stdinWrite, "some text")
				stdinWrite.Close()
				helper.CopyByteRange(0, 5)
				Expect(logfile).To(gbytes.Say("Copying bytes for table with oid 3; discarding next 0 bytes, copying 5 bytes"))
				Expect(stdout).To(gbytes.Say("some "))
			})
			It("can copy a byte range with seeking", func() {
				fmt.Fprintln(stdinWrite, "some text")
				stdinWrite.Close()
				helper.CopyByteRange(3, 5)
				Expect(logfile).To(gbytes.Say("Copying bytes for table with oid 3; discarding next 3 bytes, copying 2 bytes"))
				Expect(stdout).To(gbytes.Say("e "))
			})
		})
	})
})

package utils

import (
	"regexp"
	"strings"

	"github.com/blang/semver"
)

type GPDBVersion struct {
	VersionString string
	SemVer        semver.Version
}

func (dbversion *GPDBVersion) Initialize(dbconn *DBConn) {
	err := dbconn.Get(dbversion, "SELECT version() AS versionstring")
	CheckError(err)
	versionStart := strings.Index(dbversion.VersionString, "(Greenplum Database ") + len("(Greenplum Database ")
	versionEnd := strings.Index(dbversion.VersionString, ")")
	dbversion.VersionString = dbversion.VersionString[versionStart:versionEnd]

	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(dbversion.VersionString)[0]
	dbversion.SemVer, err = semver.Make(threeDigitVersion)
	CheckError(err)
}

func StringToSemVerRange(versionStr string) semver.Range {
	numDigits := len(strings.Split(versionStr, "."))
	if numDigits < 3 {
		versionStr += ".x"
	}
	validRange := semver.MustParseRange(versionStr)
	return validRange
}

func (dbversion GPDBVersion) Before(targetVersion string) bool {
	validRange := StringToSemVerRange("<" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) AtLeast(targetVersion string) bool {
	validRange := StringToSemVerRange(">=" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) Is(targetVersion string) bool {
	validRange := StringToSemVerRange("==" + targetVersion)
	return validRange(dbversion.SemVer)
}

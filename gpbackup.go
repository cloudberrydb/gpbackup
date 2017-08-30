// +build gpbackup

package main

import (
	. "github.com/greenplum-db/gpbackup/backup"
)

func main() {
	defer DoTeardown()
	DoInit()
	DoFlagValidation()
	DoSetup()
	DoBackup()
}

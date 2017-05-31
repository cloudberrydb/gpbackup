// +build gprestore

package main

import (
	. "gpbackup/restore"
)

func main() {
	defer DoTeardown()
	DoInit()
	DoValidation()
	DoSetup()
	DoRestore()
}

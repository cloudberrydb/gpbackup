package main

import (
	. "gpbackup/backup"
)

func main() {
	defer DoTeardown()
	DoInit()
	DoValidation()
	DoSetup()
	DoBackup()
}

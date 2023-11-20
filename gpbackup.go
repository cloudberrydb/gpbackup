// +build gpbackup

package main

import (
	"os"

	. "github.com/cloudberrydb/gpbackup/backup"
	"github.com/cloudberrydb/gpbackup/options"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "gpbackup",
		Short:   "gpbackup is the parallel backup utility for CloudberryDB",
		Args:    cobra.NoArgs,
		Version: GetVersion(),
		Run: func(cmd *cobra.Command, args []string) {
			defer DoTeardown()
			DoFlagValidation(cmd)
			DoSetup()
			DoBackup()
		}}
	rootCmd.SetArgs(options.HandleSingleDashes(os.Args[1:]))
	DoInit(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}

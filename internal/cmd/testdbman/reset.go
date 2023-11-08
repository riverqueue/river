package main

import (
	"github.com/spf13/cobra"
)

func init() { //nolint:gochecknoinits
	rootCmd.AddCommand(resetCmd)
}

var resetCmd = &cobra.Command{ //nolint:gochecknoglobals
	Use:   "reset",
	Short: "Drop and recreate test databases",
	Long: `
Reset the test databases, dropping the existing database(s) if they exist and
recreating them with the correct schema
`,
	Run: func(cmd *cobra.Command, args []string) {
		dropCmd.Run(cmd, args)
		createCmd.Run(cmd, args)
	},
}

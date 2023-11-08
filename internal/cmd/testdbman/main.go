package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// testdbman is a command-line tool for managing the test databases used by
// parallel tests and the sample applications.

var rootCmd = &cobra.Command{ //nolint:gochecknoglobals
	Use:   "testdbman",
	Short: "testdbman manages test databases",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("failed: %s\n", err)
		os.Exit(1)
	}
}

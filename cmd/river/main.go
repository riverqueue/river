package main

import (
	"os"

	"github.com/riverqueue/river/cmd/river/rivercli"
)

func main() {
	cli := rivercli.NewCLI(&rivercli.Config{
		Name: "River",
	})

	if err := cli.BaseCommandSet().Execute(); err != nil {
		// Cobra will already print an error on problems like an unknown command
		// or missing required flag. Set an exit status of 1 on error, but don't
		// print it again.
		os.Exit(1)
	}
}

package main

import (
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/cmd/river/rivercli"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type DriverProcurer struct{}

func (p *DriverProcurer) ProcurePgxV5(pool *pgxpool.Pool) riverdriver.Driver[pgx.Tx] {
	return riverpgxv5.New(pool)
}

func main() {
	cli := rivercli.NewCLI(&rivercli.Config{
		DriverProcurer: &DriverProcurer{},
		Name:           "River",
	})

	if err := cli.BaseCommandSet().Execute(); err != nil {
		// Cobra will already print an error on problems like an unknown command
		// or missing required flag. Set an exit status of 1 on error, but don't
		// print it again.
		os.Exit(1)
	}
}

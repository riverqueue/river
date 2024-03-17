package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateTestDBNames(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{
		"river_testdb",
		"river_testdb_example",
		"river_testdb_0",
		"river_testdb_1",
		"river_testdb_2",
		"river_testdb_3",
	}, generateTestDBNames(4))
}

//
// Command bundle framework
//

func TestCommandBundle(t *testing.T) {
	t.Parallel()

	var (
		baseArgs = []string{"fake-program-name"} // os args always include a program name
		ctx      = context.Background()
	)

	type testBundle struct {
		buf             *bytes.Buffer
		command1Invoked *bool
		command2Invoked *bool
	}

	setup := func() (*CommandBundle, *testBundle) {
		commandBundle := NewCommandBundle(
			"testcom",
			"testcom is a test bundle for use tests",
			`
A test only program for testing the command bundle framework, especially some
complexities around how it emits output. This is the long description and is
meant to be wrapped at 80 characters in your editor.

It may be multiple paragraphs. This is a second paragraph with additional
information and context.
		`,
		)

		var command1Invoked bool
		{
			commandBundle.AddCommand(
				"command1",
				"The program's first command",
				`
This is a long description for the program's first command. It acts somewhat
like a mock, setting a boolean to true that we can easily check in tests in case
the program makes the decision to invoke it.
`,
				func(ctx context.Context, out io.Writer) error {
					fmt.Fprintf(out, "command1 executed\n")
					command1Invoked = true
					return nil
				},
			)
		}

		var command2Invoked bool
		{
			commandBundle.AddCommand(
				"command2",
				"The program's second command",
				`
This is a long description for the program's second command. It's the same as
the first command, and acts somewhat like a mock, setting a boolean to true that
we can easily check in tests in case the program makes the decision to invoke
it.
`,
				func(ctx context.Context, out io.Writer) error {
					fmt.Fprintf(out, "command2 executed\n")
					command2Invoked = true
					return nil
				},
			)
		}
		{
			commandBundle.AddCommand(
				"makeerror",
				"A command that returns an error",
				`
The long description for a command that returns an error that we can check
against in tests to make sure that piece of the puzzle works as expected.
`,
				func(ctx context.Context, out io.Writer) error {
					fmt.Fprintf(out, "makeerror executed\n")
					return errors.New("command error!")
				},
			)
		}

		var buf bytes.Buffer
		commandBundle.out = &buf

		return commandBundle, &testBundle{
			buf:             &buf,
			command1Invoked: &command1Invoked,
			command2Invoked: &command2Invoked,
		}
	}

	expectedCommandBundleUsage := strings.TrimSpace(`
A test only program for testing the command bundle framework, especially some
complexities around how it emits output. This is the long description and is
meant to be wrapped at 80 characters in your editor.

It may be multiple paragraphs. This is a second paragraph with additional
information and context.

Usage:
  testcom [command] [flags]

Available Commands:
  command1   The program's first command
  command2   The program's second command
  makeerror  A command that returns an error

Flags:
  -help
        help for program or command

Use "testcom [command] -help" for more information about a command.
	`) + "\n"

	t.Run("ShowsUsageWithHelpArgument", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "-help")))

		require.Equal(t, expectedCommandBundleUsage, bundle.buf.String())
	})

	t.Run("ShowsUsageWithHelpCommand", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "help")))

		require.Equal(t, expectedCommandBundleUsage, bundle.buf.String())
	})

	t.Run("ShowsUsageWithNoArguments", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, baseArgs))

		require.Equal(t, expectedCommandBundleUsage, bundle.buf.String())
	})

	expectedCommand1Usage := strings.TrimSpace(`
This is a long description for the program's first command. It acts somewhat
like a mock, setting a boolean to true that we can easily check in tests in case
the program makes the decision to invoke it.

Usage:
  testcom command1 [flags]

Flags:
  -help
        help for program or command
	`) + "\n"

	t.Run("ShowsCommandUsageWithHelpArgument", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "command1", "-help")))

		require.Equal(t, expectedCommand1Usage, bundle.buf.String())
	})

	t.Run("ShowsCommandUsageWithHelpCommand", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "help", "command1")))

		require.Equal(t, expectedCommand1Usage, bundle.buf.String())
	})

	t.Run("ShowsCommandUsageWithMisorderedHelpArgument", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "-help", "command1")))

		require.Equal(t, expectedCommand1Usage, bundle.buf.String())
	})

	t.Run("ErrorsOnTooManyArguments", func(t *testing.T) {
		t.Parallel()

		commandBundle, _ := setup()

		require.EqualError(t, commandBundle.Exec(ctx, append(baseArgs, "command1", "command2")),
			"expected exactly one command")
	})

	t.Run("ErrorsOnUnknownCommand", func(t *testing.T) {
		t.Parallel()

		commandBundle, _ := setup()

		require.EqualError(t, commandBundle.Exec(ctx, append(baseArgs, "command3")),
			"unknown command: command3")
	})

	t.Run("RunsCommand", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "command1")))
		require.True(t, *bundle.command1Invoked)
		require.False(t, *bundle.command2Invoked)

		require.Equal(t, "command1 executed\n", bundle.buf.String())
	})

	t.Run("DisambiguatesCommands", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.NoError(t, commandBundle.Exec(ctx, append(baseArgs, "command2")))
		require.False(t, *bundle.command1Invoked)
		require.True(t, *bundle.command2Invoked)

		require.Equal(t, "command2 executed\n", bundle.buf.String())
	})

	t.Run("ReturnsErrorFromCommand", func(t *testing.T) {
		t.Parallel()

		commandBundle, bundle := setup()

		require.EqualError(t, commandBundle.Exec(ctx, append(baseArgs, "makeerror")), "command error!")

		require.Equal(t, "makeerror executed\n", bundle.buf.String())
	})
}

// Command taskforge-cli is the wiring for internal/cli (AGENTS.md section
// 3: cmd/ is wiring only, no domain logic). Argument parsing, HTTP calls,
// output, and the exit-code contract all live in internal/cli, where they
// can be tested without an OS process boundary.
package main

import (
	"context"
	"os"

	"github.com/co-rtex/TaskForge/internal/cli"
)

func main() {
	os.Exit(cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

package main

import (
	"io"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
)

// demo is one run: the stack it started and what it has concluded so far.
// Everything about processes, environments, ports, the broker queue and the
// run's credentials lives in scripts/internal/stack, which scripts/bench shares.
type demo struct {
	*stack.Stack
	mode string
	rep  *report
}

func newDemo(out io.Writer, mode string) (*demo, error) {
	timing := stack.SuccessTimings()
	if mode == modeFailure {
		timing = stack.FailureTimings()
	}
	s, err := stack.New(stack.Options{Out: out, Prefix: "demo", Timing: timing})
	if err != nil {
		return nil, err
	}
	return &demo{Stack: s, mode: mode, rep: &report{}}, nil
}

package cli

import "strings"

// stringSliceFlag collects a flag repeated on the command line (e.g.
// `--capability cpu --capability gpu`) into an ordered slice, implementing
// flag.Value.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

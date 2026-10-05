package main

// Finding is one thing a scanner reported, in the terms the driver prints and an
// exception names.
type Finding struct {
	Tool string
	// ID is what an exception names. Aliases are other identifiers the tool gives
	// the same finding (a CVE beside an OSV id).
	ID      string
	Aliases []string
	// Location says where, for a person: a call site, a file and line, a package.
	Location string
	// Detail says what, in one line.
	Detail string
}

// scanResult is what parsing one tool's output yields: its findings, and a note
// about what it checked, which is printed when there are none so that "clean" says
// what was clean.
type scanResult struct {
	Findings []Finding
	Note     string
}

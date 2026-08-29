// Package migrations embeds TaskForge's forward-only SQL migrations so that
// applying the schema requires no migration binary and no files on disk.
//
// Files are named NNNN_description.sql. Once a migration has been applied
// anywhere it is immutable — the runner verifies checksums and refuses to
// continue if an applied file changed. Add a new migration instead.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

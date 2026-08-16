// Package migrations holds the numbered SQL migrations, embedded into every
// binary so no migration tool is needed at deploy time.
package migrations

import "embed"

// FS contains every NNNN_name.sql file in this directory.
//
//go:embed *.sql
var FS embed.FS

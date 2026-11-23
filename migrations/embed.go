// Package migrations embeds immutable schema migrations in the release artifact.
package migrations

import "embed"

//go:embed *.sql checksums.sha256
var Files embed.FS

const InitialVersion uint = 20261123000100

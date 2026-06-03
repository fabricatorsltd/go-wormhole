package migrations

import "strings"

// SplitStatements splits a SQL script into individual statements. Full-line
// `--` comments are removed before the split, so a semicolon inside a comment
// does not leak a bogus fragment into the statement list. Empty statements are
// dropped.
//
// This is a pragmatic splitter for generated DDL, not a full SQL parser: only
// full-line comments are removed (an inline `-- comment` after SQL on the same
// line is not), and it does not understand semicolons inside string literals or
// dollar-quoted ($$...$$) bodies. Scripts that need those should be executed as
// a single statement or split by the caller.
func SplitStatements(script string) []string {
	var kept []string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}

	var out []string
	for _, stmt := range strings.Split(strings.Join(kept, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

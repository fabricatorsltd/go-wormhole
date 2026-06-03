package migrations

import (
	"strings"
	"unicode"
)

// SplitStatements splits a SQL script into individual statements at top-level
// semicolons. It is comment- and literal-aware: a `;` inside a line comment
// (`-- ...`), a block comment (`/* ... */`), a single-quoted string
// (`'...'`, with `''` escaping a quote), or a dollar-quoted body
// (`$$...$$` / `$tag$...$tag$`) does not terminate a statement. Comments are
// removed from the emitted statements, and statements that contain only
// comments or whitespace are dropped.
//
// It is a pragmatic splitter, not a full SQL parser, and the lexing is
// Postgres-flavored. It assumes standard-conforming string literals (a
// backslash is not an escape inside `'...'`), so Postgres `E'...'` escape
// strings and MySQL-style backslash escapes are not modeled, nor are MySQL `#`
// line comments. Generated DDL uses none of these; the limits only matter for
// hand-written DML passed through the CLI.
func SplitStatements(script string) []string {
	var out []string
	var buf strings.Builder
	hasContent := false

	flush := func() {
		if s := strings.TrimSpace(buf.String()); s != "" && hasContent {
			out = append(out, s)
		}
		buf.Reset()
		hasContent = false
	}

	r := []rune(script)
	n := len(r)
	for i := 0; i < n; {
		c := r[i]

		switch {
		case c == '-' && i+1 < n && r[i+1] == '-':
			// Line comment: skip to end of line, keep the newline as a separator.
			i += 2
			for i < n && r[i] != '\n' {
				i++
			}
			if i < n {
				buf.WriteByte('\n')
				i++
			}

		case c == '/' && i+1 < n && r[i+1] == '*':
			// Block comment: skip to the closing */, leave a space separator.
			i += 2
			for i < n && !(r[i] == '*' && i+1 < n && r[i+1] == '/') {
				i++
			}
			i += 2
			buf.WriteByte(' ')

		case c == '\'':
			// Single-quoted string: copy verbatim, '' is an escaped quote.
			buf.WriteRune(c)
			i++
			for i < n {
				buf.WriteRune(r[i])
				if r[i] == '\'' {
					if i+1 < n && r[i+1] == '\'' {
						buf.WriteRune(r[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			hasContent = true

		case c == '$':
			if tag, ok := dollarTag(r, i); ok {
				// Dollar-quoted body: copy verbatim until the matching tag.
				buf.WriteString(tag)
				i += len([]rune(tag))
				for i < n {
					if r[i] == '$' {
						if t2, ok2 := dollarTag(r, i); ok2 && t2 == tag {
							buf.WriteString(tag)
							i += len([]rune(tag))
							break
						}
					}
					buf.WriteRune(r[i])
					i++
				}
				hasContent = true
			} else {
				buf.WriteRune(c)
				hasContent = true
				i++
			}

		case c == ';':
			flush()
			i++

		default:
			buf.WriteRune(c)
			if !unicode.IsSpace(c) {
				hasContent = true
			}
			i++
		}
	}
	flush()
	return out
}

// dollarTag reports whether the runes starting at i open a dollar-quote tag
// ($$ or $tag$) and returns the full tag including both dollar signs. The tag,
// when present, is an identifier (first character a letter or underscore), so a
// positional parameter like $1 is not mistaken for a dollar quote.
func dollarTag(r []rune, i int) (string, bool) {
	if i >= len(r) || r[i] != '$' {
		return "", false
	}
	j := i + 1
	if j < len(r) && (r[j] == '_' || unicode.IsLetter(r[j])) {
		j++
		for j < len(r) && (r[j] == '_' || unicode.IsLetter(r[j]) || unicode.IsDigit(r[j])) {
			j++
		}
	}
	if j < len(r) && r[j] == '$' {
		return string(r[i : j+1]), true
	}
	return "", false
}

package util

import (
	"strings"
	"unicode"
)

// ToSnake converts CamelCase/PascalCase or space-separated strings to snake_case.
// It also handles hyphens by replacing them with underscores.
func ToSnake(s string) string {
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")

	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && unicode.IsLower(rune(s[i-1])) || (i > 0 && i+1 < len(s) && unicode.IsLower(rune(s[i+1])) && unicode.IsUpper(r)) {
				b.WriteRune('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

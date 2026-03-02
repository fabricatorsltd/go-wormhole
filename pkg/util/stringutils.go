package util

import "unicode"

// ToSnake converts a string to snake_case.
func ToSnake(s string) string {
	var result []rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && result[len(result)-1] != '_' {
				result = append(result, '_')
			}
			result = append(result, unicode.ToLower(r))
		} else {
			result = append(result, r)
		}
	}
	return string(result)
}

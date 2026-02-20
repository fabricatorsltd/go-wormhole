package schema

import (
	"reflect"
	"strings"
	"sync"

	"github.com/mirkobrombin/go-foundation/pkg/tags"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
)

const tagName = "db"

var (
	parser *tags.Parser
	cache  sync.Map // reflect.Type → *model.EntityMeta
)

func init() {
	parser = tags.NewParser(tagName,
		tags.WithPairDelimiter(";"),
		tags.WithKVSeparator(":"),
		tags.WithValueDelimiter(","),
	)
}

// Parse inspects a struct (or pointer-to-struct) and returns its EntityMeta.
// Results are cached per type.
func Parse(v any) *model.EntityMeta {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return ParseType(t)
}

// ParseType builds EntityMeta from a reflect.Type. Cached.
func ParseType(t reflect.Type) *model.EntityMeta {
	if m, ok := cache.Load(t); ok {
		return m.(*model.EntityMeta)
	}

	meta := &model.EntityMeta{
		Name:   toSnake(t.Name()),
		GoType: t,
	}

	parsed := parser.ParseType(t)
	for _, fm := range parsed {
		col := fm.Get("column")
		if col == "" {
			col = toSnake(fm.Name)
		}

		sf, _ := t.FieldByName(fm.Name)
		field := model.FieldMeta{
			FieldName:  fm.Name,
			Column:     col,
			GoType:     sf.Type,
			Tags:       make(map[string]string),
			PrimaryKey: fm.Has("primary_key"),
			AutoIncr:   fm.Has("auto_increment"),
			Nullable:   fm.Has("nullable"),
			Index:      fm.Get("index"),
		}

		if v := fm.Get("type"); v != "" {
			field.Tags["type"] = v
		}
		if v := fm.Get("default"); v != "" {
			field.Tags["default"] = v
		}

		meta.Fields = append(meta.Fields, field)
		if field.PrimaryKey {
			meta.PrimaryKey = &meta.Fields[len(meta.Fields)-1]
		}
	}

	meta.BuildIndex()
	cache.Store(t, meta)
	return meta
}

// toSnake converts CamelCase to snake_case, handling acronyms
// like "UserID" → "user_id".
func toSnake(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prev := runes[i-1]
				if prev >= 'a' && prev <= 'z' {
					b.WriteByte('_')
				} else if prev >= 'A' && prev <= 'Z' && i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z' {
					b.WriteByte('_')
				}
			}
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

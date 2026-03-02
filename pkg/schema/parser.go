package schema

import (
	"reflect"
	"strings"
	"sync"

	"github.com/mirkobrombin/go-foundation/pkg/tags"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider" // New import
	"github.com/fabricatorsltd/go-wormhole/pkg/util"
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
func Parse(v any, dialect provider.Dialect) *model.EntityMeta {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return ParseType(t, dialect)
}

// ParseType builds EntityMeta from a reflect.Type. Cached.
func ParseType(t reflect.Type, dialect provider.Dialect) *model.EntityMeta {
	if m, ok := cache.Load(t); ok {
		return m.(*model.EntityMeta)
	}

	meta := &model.EntityMeta{
		Name:   util.ToSnake(t.Name()),
		GoType: t,
	}

	parsed := parser.ParseType(t)
	for _, fm := range parsed {
		col := fm.Get("column")
		if col == "" {
			sf, _ := t.FieldByName(fm.Name) // Get sf here as it's needed for dialect.ColumnName
			col = dialect.ColumnName(sf.Name)
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



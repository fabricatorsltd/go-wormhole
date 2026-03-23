package schema

import (
	"reflect"
	"sync"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/util"
	"github.com/mirkobrombin/go-foundation/pkg/tags"
)

const tagName = "db"

var (
	parser    *tags.Parser
	cache     sync.Map // reflect.Type → *model.EntityMeta
	nameCache sync.Map // entity name → *model.EntityMeta
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
		Name:   util.ToSnake(t.Name()),
		GoType: t,
	}

	parsed := parser.ParseType(t)
	for _, fm := range parsed {
		col := fm.Get("column")
		sf, _ := t.FieldByName(fm.Name)
		if col == "" {
			col = util.ToSnake(sf.Name)
		}

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
	nameCache.Store(meta.Name, meta)
	return meta
}

// LookupEntity returns cached metadata by entity name, when available.
func LookupEntity(name string) *model.EntityMeta {
	if m, ok := nameCache.Load(name); ok {
		return m.(*model.EntityMeta)
	}
	return nil
}

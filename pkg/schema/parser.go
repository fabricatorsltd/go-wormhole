package schema

import (
	"reflect"
	"strings"
	"sync"
	"time"

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

	// Navigation fields (pointer-to-struct or slice-of-pointer-to-struct) are
	// not columns; collect them for a second pass once the primary key is known.
	type navField struct {
		name string
		typ  reflect.Type
		raw  string
	}
	var navs []navField

	// Table-per-hierarchy directives, resolved after the field index is built.
	var tableOverride, discColumn, discValue string

	for _, fm := range parsed {
		sf, _ := t.FieldByName(fm.Name)
		// A `json`-tagged field is stored as a JSON column, even when its Go
		// type is a pointer/slice-of-pointer to a struct. The json directive
		// therefore takes precedence over navigation-field detection.
		if isNavigationField(sf.Type) && !fm.Has("json") {
			navs = append(navs, navField{name: fm.Name, typ: sf.Type, raw: fm.RawTag})
			continue
		}

		col := fm.Get("column")
		if col == "" {
			col = util.ToSnake(sf.Name)
		}

		// table:<name> shares this type's table with sibling types (first wins).
		if t := fm.Get("table"); t != "" && tableOverride == "" {
			tableOverride = t
		}
		// discriminator:<value> marks the single-table type column and this
		// type's value.
		if d := fm.Get("discriminator"); d != "" {
			discColumn = col
			discValue = d
		}

		field := model.FieldMeta{
			FieldName:  fm.Name,
			Column:     col,
			GoType:     sf.Type,
			Tags:       make(map[string]string),
			PrimaryKey: fm.Has("primary_key"),
			AutoIncr:   fm.Has("auto_increment"),
			Nullable:   fm.Has("nullable"),
			Index:      firstNonEmpty(fm.Get("index"), fm.Get("unique_index")),
			Indexed:    fm.Has("index") || fm.Has("unique") || fm.Has("unique_index"),
			Unique:     fm.Has("unique") || fm.Has("unique_index"),
			Computed:   fm.Has("computed"),
		}

		if v := fm.Get("type"); v != "" {
			field.Tags["type"] = v
		}
		if v := fm.Get("default"); v != "" {
			field.Tags["default"] = v
		}
		if fm.Has("json") {
			// Field is (de)serialized to/from a JSON text/blob column.
			field.Tags["json"] = "true"
		}

		if fm.Has("version") {
			// Field is an optimistic-concurrency token (integer columns only).
			field.Tags["version"] = "true"
		}

		meta.Fields = append(meta.Fields, field)
		if field.PrimaryKey {
			pk := &meta.Fields[len(meta.Fields)-1]
			meta.PrimaryKeys = append(meta.PrimaryKeys, pk)
			// PrimaryKey is the singular shortcut: the first declared key.
			if meta.PrimaryKey == nil {
				meta.PrimaryKey = pk
			}
		}
		if _, ok := field.Tags["version"]; ok && isIntegerKind(sf.Type.Kind()) {
			meta.Version = &meta.Fields[len(meta.Fields)-1]
		}
	}

	meta.BuildIndex()

	// Resolve table-per-hierarchy directives now that the field index exists.
	if tableOverride != "" {
		meta.Name = tableOverride
	}
	if discColumn != "" {
		meta.Discriminator = meta.FieldByColumn(discColumn)
		meta.DiscriminatorValue = discValue
	}

	for _, nf := range navs {
		if rel, ok := parseRelation(meta, nf.name, nf.typ, nf.raw); ok {
			meta.Relations = append(meta.Relations, rel)
		}
	}

	cache.Store(t, meta)
	// Single-table-hierarchy types share one table name, so the by-name cache
	// would let one subtype evict another. Only the per-type cache (keyed by
	// reflect.Type) is authoritative for them; callers that need a discriminated
	// type pass its meta directly rather than resolving it by name.
	if meta.Discriminator == nil {
		nameCache.Store(meta.Name, meta)
	}
	return meta
}

// isNavigationField reports whether a struct field type is an entity
// navigation (*Struct or []*Struct) rather than a scalar column.
func isNavigationField(t reflect.Type) bool {
	if relationTarget(t) == nil {
		return false
	}
	return true
}

// relationTarget returns the related struct type for a navigation field type,
// or nil if t is not a navigation. It unwraps *Struct and []*Struct (and the
// rarer []Struct), but treats time.Time and []byte as scalars.
func relationTarget(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Ptr:
		if t.Elem().Kind() == reflect.Struct && !isScalarStruct(t.Elem()) {
			return t.Elem()
		}
	case reflect.Slice:
		el := t.Elem()
		if el.Kind() == reflect.Ptr {
			el = el.Elem()
		}
		if el.Kind() == reflect.Struct && !isScalarStruct(el) {
			return el
		}
	}
	return nil
}

// isScalarStruct reports whether a struct type should be treated as a scalar
// value (mapped to a column) rather than a related entity.
func isScalarStruct(t reflect.Type) bool {
	return t == reflect.TypeOf(time.Time{})
}

// normalizeOnDelete maps an on_delete tag value to its SQL referential action,
// or "" for an empty/unknown value (the dialect default applies).
func normalizeOnDelete(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "cascade":
		return "CASCADE"
	case "set_null", "setnull", "null":
		return "SET NULL"
	case "restrict":
		return "RESTRICT"
	case "no_action", "noaction":
		return "NO ACTION"
	case "set_default", "setdefault":
		return "SET DEFAULT"
	default:
		return ""
	}
}

// firstNonEmpty returns the first non-empty string among its arguments.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// isIntegerKind reports whether k is a signed or unsigned integer kind.
func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// parseRelation builds Relation metadata for a navigation field using struct
// tags (ref/fk/join) with naming-convention fallbacks. owner must already have
// its primary key resolved.
func parseRelation(owner *model.EntityMeta, fieldName string, fieldType reflect.Type, rawTag string) (model.Relation, bool) {
	target := relationTarget(fieldType)
	if target == nil {
		return model.Relation{}, false
	}

	tagVals := map[string]string{}
	if rawTag != "" {
		for k, v := range parser.Parse(rawTag) {
			if len(v) > 0 {
				tagVals[k] = v[0]
			}
		}
	}

	ownerPK := "id"
	if owner.PrimaryKey != nil {
		ownerPK = owner.PrimaryKey.Column
	}
	ownerFK := util.ToSnake(owner.GoType.Name()) + "_id" // e.g. user_id

	rel := model.Relation{
		Field:        fieldName,
		Target:       target,
		TargetEntity: util.ToSnake(target.Name()),
		LocalKey:     ownerPK,
		ForeignKey:   ownerFK,
		OnDelete:     normalizeOnDelete(tagVals["on_delete"]),
	}

	isSlice := fieldType.Kind() == reflect.Slice
	join := tagVals["join"]

	switch {
	case isSlice && join != "":
		rel.Kind = model.RelationManyToMany
		rel.JoinTable = join
		rel.JoinLocalKey = ownerFK
		rel.JoinForeignKey = util.ToSnake(target.Name()) + "_id"
		rel.ForeignKey = "id" // target PK by convention; refined at load time
		if v := tagVals["ref"]; v != "" {
			rel.JoinLocalKey = v
		}
		if v := tagVals["fk"]; v != "" {
			rel.JoinForeignKey = v
		}
	case isSlice:
		rel.Kind = model.RelationOneToMany
		if v := tagVals["fk"]; v != "" {
			rel.ForeignKey = v
		}
		if v := tagVals["ref"]; v != "" {
			rel.LocalKey = v
		}
	default: // *Struct
		// BelongsTo when the owner carries a <NavField>ID or <Target>ID column.
		if local := belongsToLocalKey(owner, fieldName, target); local != "" {
			rel.Kind = model.RelationBelongsTo
			rel.LocalKey = local
			rel.ForeignKey = "id" // target PK by convention; refined at load time
		} else {
			rel.Kind = model.RelationOneToOne
		}
		if v := tagVals["fk"]; v != "" {
			rel.ForeignKey = v
		}
		if v := tagVals["ref"]; v != "" {
			rel.LocalKey = v
		}
	}

	return rel, true
}

// belongsToLocalKey returns the owner column that acts as the foreign key to
// target. It looks for a <NavField>ID field first (e.g. User -> UserID), then a
// <Target>ID field, returning "" if neither exists.
func belongsToLocalKey(owner *model.EntityMeta, navField string, target reflect.Type) string {
	for _, want := range []string{navField + "ID", target.Name() + "ID"} {
		if f := owner.Field(want); f != nil {
			return f.Column
		}
	}
	return ""
}

// LookupEntity returns cached metadata by entity name, when available.
func LookupEntity(name string) *model.EntityMeta {
	if m, ok := nameCache.Load(name); ok {
		return m.(*model.EntityMeta)
	}
	return nil
}

package context

import (
	stdctx "context"
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// loadInclude eager-loads a single navigation relation onto the already-loaded
// parent results in s.dest, using one batched "WHERE key IN (...)" query.
//
// Strategy (split query, not JOIN):
//  1. Collect the parents' link-key values.
//  2. Load all related rows whose key is in that set in one query.
//  3. Group the children by their link key and assign them onto each parent's
//     navigation field via reflection.
//
// One-to-one, one-to-many and belongs-to are supported. Many-to-many requires a
// join-table hop and is not yet supported.
func (s *EntitySet) loadInclude(ctx stdctx.Context, name string) error {
	rel := s.meta.Relation(name)
	if rel == nil {
		return fmt.Errorf("Include(%q): %q has no relation %q", name, s.meta.Name, name)
	}
	if rel.Kind == model.RelationManyToMany {
		return fmt.Errorf("Include(%q): many-to-many eager loading is not yet supported", name)
	}

	parents := derefSlice(s.dest)
	if !parents.IsValid() || parents.Len() == 0 {
		return nil
	}

	childMeta := schema.ParseType(rel.Target)

	// parentKeyCol is read from each parent; childKeyCol links a child back to
	// its parent. For has-one / has-many the child carries the FK; for
	// belongs-to the parent carries the FK and the child is matched on its PK.
	parentKeyCol := rel.LocalKey
	childKeyCol := rel.ForeignKey

	parentKeyField := fieldNameForColumn(s.meta, parentKeyCol)
	childKeyField := fieldNameForColumn(childMeta, childKeyCol)
	if parentKeyField == "" || childKeyField == "" {
		return fmt.Errorf("Include(%q): cannot resolve link columns (%s/%s)", name, parentKeyCol, childKeyCol)
	}

	// Collect distinct parent key values. Keys are normalized to a canonical
	// type so an int64 primary key matches an int foreign key (a raw any-keyed
	// map would compare the dynamic type too and silently miss).
	keySet := make(map[any]struct{})
	var keys []any
	for i := 0; i < parents.Len(); i++ {
		v := fieldValue(parents.Index(i), parentKeyField)
		if !v.IsValid() {
			continue
		}
		k := normalizeKey(v)
		if _, seen := keySet[k]; !seen {
			keySet[k] = struct{}{}
			keys = append(keys, v.Interface()) // query with the field's real type
		}
	}
	if len(keys) == 0 {
		return nil
	}

	// Load children: SELECT * FROM child WHERE childKeyCol IN (keys).
	childSlicePtr := reflect.New(reflect.SliceOf(reflect.PtrTo(rel.Target))) // *[]*Target
	q := query.Query{
		EntityName: childMeta.Name,
		Where:      query.Predicate{Field: childKeyCol, Op: query.OpIn, Value: keys},
	}
	if err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, childMeta, q, childSlicePtr.Interface())
	}); err != nil {
		return fmt.Errorf("Include(%q): %w", name, err)
	}

	// Group children by their link-key value.
	children := childSlicePtr.Elem() // []*Target
	grouped := make(map[any][]reflect.Value, len(keys))
	for i := 0; i < children.Len(); i++ {
		child := children.Index(i) // *Target
		kv := fieldValue(child.Elem(), childKeyField)
		if !kv.IsValid() {
			continue
		}
		k := normalizeKey(kv)
		grouped[k] = append(grouped[k], child)
	}

	// Assign onto each parent's navigation field.
	collection := rel.Kind == model.RelationOneToMany
	for i := 0; i < parents.Len(); i++ {
		parent := parents.Index(i)
		pkv := fieldValue(parent, parentKeyField)
		if !pkv.IsValid() {
			continue
		}
		matches := grouped[normalizeKey(pkv)]
		if err := assignNav(addressableStruct(parent), rel.Field, matches, collection); err != nil {
			return fmt.Errorf("Include(%q): %w", name, err)
		}
	}
	return nil
}

// derefSlice resolves dest (*[]T or *[]*T) to the underlying slice Value.
func derefSlice(dest any) reflect.Value {
	v := reflect.ValueOf(dest)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return reflect.Value{}
	}
	return v
}

// addressableStruct returns the settable struct Value for a slice element that
// may be a struct (T) or a pointer to struct (*T).
func addressableStruct(elem reflect.Value) reflect.Value {
	if elem.Kind() == reflect.Ptr {
		return elem.Elem()
	}
	return elem
}

// fieldValue reads a struct field by Go name from a struct or *struct Value.
func fieldValue(v reflect.Value, field string) reflect.Value {
	v = addressableStruct(v)
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	return v.FieldByName(field)
}

// assignNav sets a navigation field to the matched children. For a collection
// the field is []*Target (or []Target); otherwise it is *Target (or Target),
// set to the first match.
func assignNav(parent reflect.Value, field string, matches []reflect.Value, collection bool) error {
	f := parent.FieldByName(field)
	if !f.IsValid() || !f.CanSet() {
		return fmt.Errorf("navigation field %q is not settable", field)
	}

	if collection {
		out := reflect.MakeSlice(f.Type(), 0, len(matches))
		elemIsPtr := f.Type().Elem().Kind() == reflect.Ptr
		for _, m := range matches { // m is *Target
			if elemIsPtr {
				out = reflect.Append(out, m)
			} else {
				out = reflect.Append(out, m.Elem())
			}
		}
		f.Set(out)
		return nil
	}

	if len(matches) == 0 {
		return nil
	}
	m := matches[0] // *Target
	if f.Kind() == reflect.Ptr {
		f.Set(m)
	} else {
		f.Set(m.Elem())
	}
	return nil
}

// normalizeKey reduces a link-key value to a width-agnostic, comparable form so
// that keys of different integer widths (e.g. int64 PK vs int FK) match when
// used as map keys. Signed integers collapse to int64, unsigned to uint64,
// floats to float64; everything else keeps its own comparable value.
func normalizeKey(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		return v.Interface()
	}
}

// fieldNameForColumn maps a storage column back to its Go field name.
func fieldNameForColumn(meta *model.EntityMeta, column string) string {
	if f := meta.FieldByColumn(column); f != nil {
		return f.FieldName
	}
	if f := meta.Field(column); f != nil {
		return f.FieldName
	}
	return ""
}

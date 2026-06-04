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
// One-to-one, one-to-many and belongs-to load with a single batched query;
// many-to-many takes a second hop through the join table.
func (s *EntitySet) loadInclude(ctx stdctx.Context, name string) error {
	rel := s.meta.Relation(name)
	if rel == nil {
		return fmt.Errorf("Include(%q): %q has no relation %q", name, s.meta.Name, name)
	}

	parents := derefSlice(s.dest)
	if !parents.IsValid() || parents.Len() == 0 {
		return nil
	}

	if rel.Kind == model.RelationManyToMany {
		return s.loadManyToMany(ctx, name, rel, parents)
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

	// Load children: SELECT * FROM child WHERE childKeyCol IN (keys), with the
	// child entity's registered query filters ANDed in so eager-loaded
	// relations stay scoped (e.g. a tenant filter is not bypassed via Include).
	childSlicePtr := reflect.New(reflect.SliceOf(reflect.PtrTo(rel.Target))) // *[]*Target
	var where query.Node = query.Predicate{Field: childKeyCol, Op: query.OpIn, Value: keys}
	if !s.ignoreFilters {
		if f := s.ctx.filtersFor(childMeta.Name); len(f) > 0 {
			where = query.Composite{Logic: query.LogicAnd, Children: append([]query.Node{where}, f...)}
		}
	}
	q := query.Query{
		EntityName: childMeta.Name,
		Where:      where,
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

// loadManyToMany eager-loads a many-to-many relation via its join table:
//  1. collect the owners' keys,
//  2. read (owner, target) pairs from the join table for those keys,
//  3. load the targets referenced by those pairs,
//  4. stitch the targets onto each owner's navigation slice.
//
// The join table need not be a registered Go entity: a two-column row type is
// synthesized to scan the link rows.
func (s *EntitySet) loadManyToMany(ctx stdctx.Context, name string, rel *model.Relation, parents reflect.Value) error {
	childMeta := schema.ParseType(rel.Target)

	// The target is matched on its real primary key, resolved here rather than
	// assuming the "id" convention the parser defaults to.
	targetPKCol := rel.ForeignKey
	if childMeta.PrimaryKey != nil {
		targetPKCol = childMeta.PrimaryKey.Column
	}

	ownerKeyField := fieldNameForColumn(s.meta, rel.LocalKey)
	targetKeyField := fieldNameForColumn(childMeta, targetPKCol)
	ownerKeyType := goTypeOfColumn(s.meta, rel.LocalKey)
	targetKeyType := goTypeOfColumn(childMeta, targetPKCol)
	if ownerKeyField == "" || targetKeyField == "" || ownerKeyType == nil || targetKeyType == nil {
		return fmt.Errorf("Include(%q): cannot resolve many-to-many link columns", name)
	}

	// 1. Distinct owner keys.
	ownerSeen := map[any]struct{}{}
	var ownerKeys []any
	for i := 0; i < parents.Len(); i++ {
		v := fieldValue(parents.Index(i), ownerKeyField)
		if !v.IsValid() {
			continue
		}
		k := normalizeKey(v)
		if _, ok := ownerSeen[k]; ok {
			continue
		}
		ownerSeen[k] = struct{}{}
		ownerKeys = append(ownerKeys, v.Interface())
	}
	if len(ownerKeys) == 0 {
		return nil
	}

	// 2. Join rows: a synthetic two-column row (Local, Foreign) scanned from the
	// join table where the owner column is in the collected keys.
	joinRowType := reflect.StructOf([]reflect.StructField{
		{Name: "Local", Type: ownerKeyType},
		{Name: "Foreign", Type: targetKeyType},
	})
	joinMeta := &model.EntityMeta{
		Name: rel.JoinTable,
		Fields: []model.FieldMeta{
			{FieldName: "Local", Column: rel.JoinLocalKey, GoType: ownerKeyType},
			{FieldName: "Foreign", Column: rel.JoinForeignKey, GoType: targetKeyType},
		},
	}
	joinMeta.BuildIndex()
	joinRows := reflect.New(reflect.SliceOf(joinRowType)) // *[]joinRow
	jq := query.Query{
		EntityName: rel.JoinTable,
		Where:      query.Predicate{Field: rel.JoinLocalKey, Op: query.OpIn, Value: ownerKeys},
	}
	if err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, joinMeta, jq, joinRows.Interface())
	}); err != nil {
		return fmt.Errorf("Include(%q): join table: %w", name, err)
	}

	// 3. Owner -> target keys, and the distinct target keys to load.
	ownerToTargets := map[any][]any{}
	targetSeen := map[any]struct{}{}
	var targetKeys []any
	rows := joinRows.Elem()
	for i := 0; i < rows.Len(); i++ {
		local := rows.Index(i).FieldByName("Local")
		foreign := rows.Index(i).FieldByName("Foreign")
		ownerToTargets[normalizeKey(local)] = append(ownerToTargets[normalizeKey(local)], foreign.Interface())
		fk := normalizeKey(foreign)
		if _, seen := targetSeen[fk]; seen {
			continue
		}
		targetSeen[fk] = struct{}{}
		targetKeys = append(targetKeys, foreign.Interface())
	}

	// 4. Load the targets (applying their query filters), keyed by PK.
	byTargetKey := map[any]reflect.Value{}
	if len(targetKeys) > 0 {
		targetSlice := reflect.New(reflect.SliceOf(reflect.PtrTo(rel.Target))) // *[]*Target
		var where query.Node = query.Predicate{Field: targetPKCol, Op: query.OpIn, Value: targetKeys}
		if !s.ignoreFilters {
			if f := s.ctx.filtersFor(childMeta.Name); len(f) > 0 {
				where = query.Composite{Logic: query.LogicAnd, Children: append([]query.Node{where}, f...)}
			}
		}
		tq := query.Query{EntityName: childMeta.Name, Where: where}
		if err := s.ctx.withReadResilience(ctx, func() error {
			return s.ctx.provider.Execute(ctx, childMeta, tq, targetSlice.Interface())
		}); err != nil {
			return fmt.Errorf("Include(%q): %w", name, err)
		}
		ts := targetSlice.Elem()
		for i := 0; i < ts.Len(); i++ {
			t := ts.Index(i)
			byTargetKey[normalizeKey(fieldValue(t.Elem(), targetKeyField))] = t
		}
	}

	// 5. Stitch onto each owner's navigation slice.
	for i := 0; i < parents.Len(); i++ {
		parent := parents.Index(i)
		pk := fieldValue(parent, ownerKeyField)
		if !pk.IsValid() {
			continue
		}
		var matches []reflect.Value
		for _, tk := range ownerToTargets[normalizeKey(pk)] {
			if t, ok := byTargetKey[normalizeKey(reflect.ValueOf(tk))]; ok {
				matches = append(matches, t)
			}
		}
		if err := assignNav(addressableStruct(parent), rel.Field, matches, true); err != nil {
			return fmt.Errorf("Include(%q): %w", name, err)
		}
	}
	return nil
}

// goTypeOfColumn returns the Go type of the field mapped to a storage column.
func goTypeOfColumn(meta *model.EntityMeta, column string) reflect.Type {
	if f := meta.FieldByColumn(column); f != nil {
		return f.GoType
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

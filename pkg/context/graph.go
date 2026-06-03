package context

import (
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// planInsertOrder returns an ordering of indices into pending such that, for
// every foreign-key dependency between two Added entities held in memory, the
// parent (referenced) entity is applied before the child. Ordering constraints
// come only from in-memory navigation pointers among Added entries; all other
// entries keep their original relative position.
//
// The sort is stable: with no FK edges it returns 0..n-1 unchanged, so existing
// flush behaviour is untouched. A dependency cycle (e.g. User.Profile <->
// Profile.User both new) returns an error rather than looping.
func planInsertOrder(pending []*model.Entry) ([]int, error) {
	n := len(pending)

	// Map each Added entity's pointer to its index for edge resolution.
	idxByPtr := make(map[uintptr]int, n)
	for i, e := range pending {
		if e.State == model.Added {
			if p := entityPtr(e.Entity); p != 0 {
				idxByPtr[p] = i
			}
		}
	}

	adj := make([][]int, n)   // parent -> children
	indeg := make([]int, n)   // number of unmet parents
	addEdge := func(parent, child int) {
		if parent == child {
			return
		}
		adj[parent] = append(adj[parent], child)
		indeg[child]++
	}

	for i, e := range pending {
		if e.State != model.Added {
			continue
		}
		for _, rel := range e.Meta.Relations {
			switch rel.Kind {
			case model.RelationBelongsTo:
				// e (child) references a parent: parent must insert first.
				if p := navPointer(e.Entity, rel.Field); p != 0 {
					if j, ok := idxByPtr[p]; ok {
						addEdge(j, i)
					}
				}
			case model.RelationOneToMany, model.RelationOneToOne:
				// e (parent) owns children that carry its key: e first.
				for _, p := range navPointers(e.Entity, rel.Field) {
					if j, ok := idxByPtr[p]; ok {
						addEdge(i, j)
					}
				}
			}
		}
	}

	// Kahn's algorithm, scanning indices in original order for stability.
	order := make([]int, 0, n)
	for len(order) < n {
		progressed := false
		for i := 0; i < n; i++ {
			if indeg[i] == 0 {
				order = append(order, i)
				indeg[i] = -1 // mark consumed
				for _, ch := range adj[i] {
					indeg[ch]--
				}
				progressed = true
			}
		}
		if !progressed {
			return nil, fmt.Errorf("cyclic foreign-key dependency among new entities; insert them in separate SaveChanges calls")
		}
	}
	return order, nil
}

// fixupBelongsToFKs copies the primary key of each in-memory parent referenced
// by a BelongsTo navigation into this entity's foreign-key column. Called for
// an Added entity immediately before it is inserted, after its parents have
// been inserted (so their auto-increment PKs are populated).
func (c *DbContext) fixupBelongsToFKs(e *model.Entry) {
	child := structValue(e.Entity)
	if !child.IsValid() {
		return
	}
	for _, rel := range e.Meta.Relations {
		if rel.Kind != model.RelationBelongsTo {
			continue
		}
		parent := navStruct(e.Entity, rel.Field)
		if !parent.IsValid() {
			continue
		}
		parentMeta := schema.ParseType(rel.Target)
		if parentMeta.PrimaryKey == nil {
			continue
		}
		pk := parent.FieldByName(parentMeta.PrimaryKey.FieldName)
		setField(child, fieldNameForColumn(e.Meta, rel.LocalKey), pk)
	}
}

// fixupChildFKs writes this (just-inserted) entity's primary key onto the
// foreign-key column of every Added child reachable through a OneToMany /
// OneToOne navigation. Children that are not tracked as Added are left alone:
// rewriting a Modified child's FK here would not be reflected in its already
// computed change set.
func (c *DbContext) fixupChildFKs(e *model.Entry) {
	if e.Meta.PrimaryKey == nil {
		return
	}
	parent := structValue(e.Entity)
	if !parent.IsValid() {
		return
	}
	pk := parent.FieldByName(e.Meta.PrimaryKey.FieldName)
	if !pk.IsValid() {
		return
	}
	for _, rel := range e.Meta.Relations {
		if rel.Kind != model.RelationOneToMany && rel.Kind != model.RelationOneToOne {
			continue
		}
		childMeta := schema.ParseType(rel.Target)
		fkField := fieldNameForColumn(childMeta, rel.ForeignKey)
		for _, childPtr := range navInterfaces(e.Entity, rel.Field) {
			if ce, ok := c.tracker.Entry(childPtr); !ok || ce.State != model.Added {
				continue
			}
			setField(structValue(childPtr), fkField, pk)
		}
	}
}

// bumpVersionTokens advances the in-memory optimistic-concurrency version of
// every Modified entity by one, matching the server-side `version = version + 1`
// applied during flush. Called only after the transaction commits.
func bumpVersionTokens(pending []*model.Entry) {
	for _, e := range pending {
		if e.State != model.Modified || e.Meta.Version == nil {
			continue
		}
		sv := structValue(e.Entity)
		if !sv.IsValid() {
			continue
		}
		f := sv.FieldByName(e.Meta.Version.FieldName)
		if !f.IsValid() || !f.CanSet() {
			continue
		}
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(f.Int() + 1)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(f.Uint() + 1)
		}
	}
}

// --- reflection helpers ---

func entityPtr(entity any) uintptr {
	v := reflect.ValueOf(entity)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0
	}
	return v.Pointer()
}

// structValue returns the settable struct Value behind a pointer-to-struct.
func structValue(entity any) reflect.Value {
	v := reflect.ValueOf(entity)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	return v
}

// navStruct returns the struct behind a *Struct navigation field, or invalid.
func navStruct(entity any, field string) reflect.Value {
	sv := structValue(entity)
	if !sv.IsValid() {
		return reflect.Value{}
	}
	f := sv.FieldByName(field)
	if !f.IsValid() || f.Kind() != reflect.Ptr || f.IsNil() {
		return reflect.Value{}
	}
	return f.Elem()
}

// navPointer returns the pointer address of a *Struct navigation field.
func navPointer(entity any, field string) uintptr {
	sv := structValue(entity)
	if !sv.IsValid() {
		return 0
	}
	f := sv.FieldByName(field)
	if !f.IsValid() || f.Kind() != reflect.Ptr || f.IsNil() {
		return 0
	}
	return f.Pointer()
}

// navPointers returns the pointer addresses of a []*Struct navigation field.
func navPointers(entity any, field string) []uintptr {
	var out []uintptr
	for _, it := range navInterfaces(entity, field) {
		if p := entityPtr(it); p != 0 {
			out = append(out, p)
		}
	}
	return out
}

// navInterfaces returns the elements of a navigation field as interfaces:
// for *Struct a single pointer, for []*Struct each pointer element.
func navInterfaces(entity any, field string) []any {
	sv := structValue(entity)
	if !sv.IsValid() {
		return nil
	}
	f := sv.FieldByName(field)
	if !f.IsValid() {
		return nil
	}
	switch f.Kind() {
	case reflect.Ptr:
		if f.IsNil() {
			return nil
		}
		return []any{f.Interface()}
	case reflect.Slice:
		out := make([]any, 0, f.Len())
		for i := 0; i < f.Len(); i++ {
			el := f.Index(i)
			if el.Kind() == reflect.Ptr && el.IsNil() {
				continue
			}
			out = append(out, el.Interface())
		}
		return out
	}
	return nil
}

// setField assigns src onto the named field of struct sv, converting integer
// widths as needed. No-op when the field is missing, unsettable, or the value
// is not convertible.
func setField(sv reflect.Value, fieldName string, src reflect.Value) {
	if !sv.IsValid() || fieldName == "" || !src.IsValid() {
		return
	}
	f := sv.FieldByName(fieldName)
	if !f.IsValid() || !f.CanSet() {
		return
	}
	if src.Type().ConvertibleTo(f.Type()) {
		f.Set(src.Convert(f.Type()))
	}
}

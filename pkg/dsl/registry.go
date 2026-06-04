package dsl

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/fabricatorsltd/go-wormhole/pkg/util"
	"github.com/mirkobrombin/go-foundation/pkg/tags"
)

// fieldInfo maps a memory offset to the field's identity.
type fieldInfo struct {
	Name   string // Go struct field name
	Column string // storage column name (from db tag or snake_case)
	Offset uintptr
	Type   reflect.Type
}

// typeMap holds all field offsets for a registered struct type.
//
// table is the storage table name derived from the struct type name (snake_case)
// so that predicates produced by the pointer-tracking DSL can be qualified
// with their source table for join-aware queries. The compiler emits the
// qualifier only when the query has joins; single-table SELECTs are
// unaffected.
type typeMap struct {
	fields []fieldInfo
	byOff  map[uintptr]*fieldInfo
	table  string
}

var (
	mu       sync.RWMutex
	registry = map[reflect.Type]*typeMap{}
	parser   *tags.Parser
)

func init() {
	parser = tags.NewParser("db",
		tags.WithPairDelimiter(";"),
		tags.WithKVSeparator(":"),
		tags.WithValueDelimiter(","),
	)
}

// Register inspects a struct type (pass a zero-value or pointer) and
// builds the offset→field mapping used by the pointer-tracking DSL.
// All exported fields are registered; db tags provide optional column overrides.
// Call once at boot for every domain entity.
func Register(v any) {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("dsl.Register: expected struct, got %s", t.Kind()))
	}

	mu.Lock()
	defer mu.Unlock()

	if _, ok := registry[t]; ok {
		return // already registered
	}

	tm := &typeMap{
		byOff: make(map[uintptr]*fieldInfo),
		table: util.ToSnake(t.Name()),
	}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		col := util.ToSnake(sf.Name)

		var parsedTags map[string][]string
		tagStr := sf.Tag.Get("db")
		if tagStr != "" {
			parsedTags = parser.Parse(tagStr)
			if vals := parsedTags["column"]; len(vals) > 0 {
				col = vals[0]
			}
			// A table: override (single-table hierarchy) renames the source
			// table, so qualified predicates match the real table, not the
			// struct-derived name.
			if vals := parsedTags["table"]; len(vals) > 0 && vals[0] != "" {
				tm.table = vals[0]
			}
		}

		// An owned (complex) type is flattened into the owner's columns; register
		// each leaf at its cumulative offset so field pointers like
		// &owner.Address.Street resolve to the prefixed column.
		if _, owned := parsedTags["owned"]; owned && sf.Type.Kind() == reflect.Struct {
			prefix := util.ToSnake(sf.Name) + "_"
			if vals := parsedTags["prefix"]; len(vals) > 0 {
				prefix = vals[0]
			}
			tm.registerOwned(sf, prefix)
			continue
		}

		fi := fieldInfo{
			Name:   sf.Name,
			Column: col,
			Offset: sf.Offset,
			Type:   sf.Type,
		}
		tm.fields = append(tm.fields, fi)
		tm.byOff[sf.Offset] = &tm.fields[len(tm.fields)-1]
	}

	registry[t] = tm
}

// registerOwned registers each leaf of an owned (complex) struct field at its
// cumulative offset within the entity, so a field pointer into the nested value
// object resolves to the prefixed storage column. Nesting is one level deep,
// matching the schema parser.
func (tm *typeMap) registerOwned(owner reflect.StructField, prefix string) {
	for i := 0; i < owner.Type.NumField(); i++ {
		sf := owner.Type.Field(i)
		if !sf.IsExported() {
			continue
		}
		col := util.ToSnake(sf.Name)
		if tagStr := sf.Tag.Get("db"); tagStr != "" {
			if vals := parser.Parse(tagStr)["column"]; len(vals) > 0 {
				col = vals[0]
			}
		}
		off := owner.Offset + sf.Offset
		fi := fieldInfo{
			Name:   owner.Name + "." + sf.Name,
			Column: prefix + col,
			Offset: off,
			Type:   sf.Type,
		}
		tm.fields = append(tm.fields, fi)
		tm.byOff[off] = &tm.fields[len(tm.fields)-1]
	}
}

// MustRegister is like Register but also returns the zero-value pointer
// for chaining: `u := dsl.MustRegister(&User{})`.
func MustRegister[T any](v *T) *T {
	Register(v)
	return v
}

// lookup returns the typeMap for a registered struct type.
func lookup(t reflect.Type) *typeMap {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	mu.RLock()
	defer mu.RUnlock()
	return registry[t]
}

// FieldName returns the Go field name for a field pointer.
//
//	u := &User{}
//	dsl.FieldName(u, &u.Age) // → "Age"
func FieldName[B any, F any](base *B, fieldPtr *F) string {
	fi := resolveOrPanic(base, fieldPtr)
	return fi.Name
}

// ColumnName returns the storage column name for a field pointer.
//
//	u := &User{}
//	dsl.ColumnName(u, &u.CreatedAt) // → "created_at"
func ColumnName[B any, F any](base *B, fieldPtr *F) string {
	fi := resolveOrPanic(base, fieldPtr)
	return fi.Column
}

func resolveOrPanic[B any, F any](base *B, fieldPtr *F) *fieldInfo {
	baseAddr := uintptr(unsafe.Pointer(base))
	fieldAddr := uintptr(unsafe.Pointer(fieldPtr))
	offset := fieldAddr - baseAddr

	tm := lookup(reflect.TypeOf(base).Elem())
	if tm == nil {
		panic("dsl: type not registered — call dsl.Register first")
	}
	fi, ok := tm.byOff[offset]
	if !ok {
		panic(fmt.Sprintf("dsl: offset %d not found", offset))
	}
	return fi
}

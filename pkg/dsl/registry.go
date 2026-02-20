package dsl

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

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
type typeMap struct {
	fields []fieldInfo
	byOff  map[uintptr]*fieldInfo
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

	tm := &typeMap{byOff: make(map[uintptr]*fieldInfo)}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		col := toSnake(sf.Name)

		tagStr := sf.Tag.Get("db")
		if tagStr != "" {
			parsedTags := parser.Parse(tagStr)
			if vals := parsedTags["column"]; len(vals) > 0 {
				col = vals[0]
			}
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

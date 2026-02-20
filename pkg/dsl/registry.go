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
	parsed := parser.ParseType(t)

	for _, fm := range parsed {
		sf, ok := t.FieldByName(fm.Name)
		if !ok || !sf.IsExported() {
			continue
		}

		col := fm.Get("column")
		if col == "" {
			col = toSnake(fm.Name)
		}

		fi := fieldInfo{
			Name:   fm.Name,
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

// resolveField converts a pointer-to-field into its fieldInfo by
// computing the offset from the struct base address.
//
// Usage: given `u := &User{}`, calling resolveField(u, &u.Age) finds
// the Age field via `uintptr(&u.Age) - uintptr(u)`.
func resolveField(base any, fieldPtr any) (*fieldInfo, error) {
	baseVal := reflect.ValueOf(base)
	if baseVal.Kind() != reflect.Ptr || baseVal.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("base must be *struct")
	}

	baseAddr := baseVal.Pointer()
	fieldAddr := reflect.ValueOf(fieldPtr).Pointer()
	offset := fieldAddr - baseAddr

	tm := lookup(baseVal.Type())
	if tm == nil {
		return nil, fmt.Errorf("type %s not registered — call dsl.Register first", baseVal.Type().Elem().Name())
	}

	fi, ok := tm.byOff[offset]
	if !ok {
		return nil, fmt.Errorf("offset %d not found in %s", offset, baseVal.Type().Elem().Name())
	}
	return fi, nil
}

// FieldName returns the Go field name for a field pointer.
//
//	u := &User{}
//	dsl.FieldName(u, &u.Age) // → "Age"
func FieldName(base any, fieldPtr any) string {
	fi, err := resolveField(base, fieldPtr)
	if err != nil {
		panic(err)
	}
	return fi.Name
}

// ColumnName returns the storage column name for a field pointer.
//
//	u := &User{}
//	dsl.ColumnName(u, &u.CreatedAt) // → "created_at"
func ColumnName(base any, fieldPtr any) string {
	fi, err := resolveField(base, fieldPtr)
	if err != nil {
		panic(err)
	}
	return fi.Column
}

// unsafeResolve is the fast path used internally by condition constructors.
// It uses unsafe.Pointer to avoid reflect overhead on the hot path.
func unsafeResolve(base unsafe.Pointer, fieldPtr unsafe.Pointer, baseType reflect.Type) (*fieldInfo, bool) {
	offset := uintptr(fieldPtr) - uintptr(base)
	tm := lookup(baseType)
	if tm == nil {
		return nil, false
	}
	fi, ok := tm.byOff[offset]
	return fi, ok
}

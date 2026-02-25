package dsl

import (
	"fmt"

	"github.com/mirkobrombin/go-foundation/pkg/pointer"
)

var (
	registry = pointer.NewRegistry("db")
)

// Register inspects a struct type (pass a zero-value or pointer) and
// builds the offset→field mapping used by the pointer-tracking DSL.
// All exported fields are registered; db tags provide optional column overrides.
// Call once at boot for every domain entity.
func Register(v any) {
	registry.Register(v)
}

// MustRegister is like Register but also returns the zero-value pointer
// for chaining: `u := dsl.MustRegister(&User{})`.
func MustRegister[T any](v *T) *T {
	Register(v)
	return v
}

// FieldName returns the Go field name for a field pointer.
//
//	u := &User{}
//	dsl.FieldName(u, &u.Age) // → "Age"
func FieldName[B any, F any](base *B, fieldPtr *F) string {
	return pointer.FieldName(registry, base, fieldPtr)
}

// ColumnName returns the storage column name for a field pointer.
//
//	u := &User{}
//	dsl.ColumnName(u, &u.CreatedAt) // → "created_at"
func ColumnName[B any, F any](base *B, fieldPtr *F) string {
	col := pointer.TagValue(registry, base, fieldPtr, "column")
	if col != "" {
		return col
	}
	return toSnake(pointer.FieldName(registry, base, fieldPtr))
}

func resolveOrPanic[B any, F any](base *B, fieldPtr *F) (string, string) {
	fi, err := registry.Resolve(base, fieldPtr)
	if err != nil {
		panic(fmt.Sprintf("dsl: %v — call dsl.Register first", err))
	}
	col := ""
	if vals, ok := fi.Tags["column"]; ok && len(vals) > 0 {
		col = vals[0]
	} else {
		col = toSnake(fi.Name)
	}
	return fi.Name, col
}

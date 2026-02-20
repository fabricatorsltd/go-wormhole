package dsl

import (
	"github.com/mirkobrombin/go-wormhole/pkg/query"
)

// Condition wraps a query.Predicate produced by the pointer-tracking
// DSL. It is the value passed to Builder.Filter().
type Condition = query.Predicate

// Eq builds an equality predicate: WHERE field = val.
//
//	u := &User{}
//	dsl.Eq(u, &u.Age, 18)
func Eq[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpEq, val)
}

// Neq builds a not-equal predicate.
func Neq[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpNeq, val)
}

// Gt builds a greater-than predicate.
func Gt[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpGt, val)
}

// Gte builds a greater-or-equal predicate.
func Gte[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpGte, val)
}

// Lt builds a less-than predicate.
func Lt[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpLt, val)
}

// Lte builds a less-or-equal predicate.
func Lte[T any](base any, fieldPtr *T, val T) Condition {
	return cond(base, fieldPtr, query.OpLte, val)
}

// In builds an IN predicate.
func In[T any](base any, fieldPtr *T, vals ...T) Condition {
	fi := mustResolve(base, fieldPtr)
	items := make([]any, len(vals))
	for i, v := range vals {
		items[i] = v
	}
	return Condition{Field: fi.Name, Op: query.OpIn, Value: items}
}

// Like builds a LIKE predicate (raw pattern, e.g. "%alice%").
func Like(base any, fieldPtr *string, pattern string) Condition {
	fi := mustResolve(base, fieldPtr)
	return Condition{Field: fi.Name, Op: query.OpLike, Value: pattern}
}

// Contains builds a LIKE %val% predicate.
func Contains(base any, fieldPtr *string, val string) Condition {
	fi := mustResolve(base, fieldPtr)
	return Condition{Field: fi.Name, Op: query.OpLike, Value: "%" + val + "%"}
}

// IsNil builds an IS NULL predicate.
func IsNil[T any](base any, fieldPtr *T) Condition {
	fi := mustResolve(base, fieldPtr)
	return Condition{Field: fi.Name, Op: query.OpIsNil, Value: nil}
}

// --- internal ---

func cond[T any](base any, fieldPtr *T, op query.Op, val T) Condition {
	fi := mustResolve(base, fieldPtr)
	return Condition{Field: fi.Name, Op: op, Value: val}
}

func mustResolve[T any](base any, fieldPtr *T) *fieldInfo {
	fi, err := resolveField(base, fieldPtr)
	if err != nil {
		panic(err)
	}
	return fi
}

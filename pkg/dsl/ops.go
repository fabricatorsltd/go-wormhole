package dsl

import (
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

// Condition wraps a query.Predicate produced by the pointer-tracking
// DSL. It is the value passed to Builder.Filter().
type Condition = query.Predicate

// Eq builds an equality predicate: WHERE column = val.
//
//	u := &User{}
//	dsl.Eq(u, &u.Age, 18)
func Eq[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpEq, val)
}

// Neq builds a not-equal predicate.
func Neq[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpNeq, val)
}

// Gt builds a greater-than predicate.
func Gt[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpGt, val)
}

// Gte builds a greater-or-equal predicate.
func Gte[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpGte, val)
}

// Lt builds a less-than predicate.
func Lt[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpLt, val)
}

// Lte builds a less-or-equal predicate.
func Lte[B any, F any](base *B, fieldPtr *F, val F) Condition {
	return cond(base, fieldPtr, query.OpLte, val)
}

// In builds an IN predicate.
func In[B any, F any](base *B, fieldPtr *F, vals ...F) Condition {
	_, col := resolveOrPanic(base, fieldPtr)
	items := make([]any, len(vals))
	for i, v := range vals {
		items[i] = v
	}
	return Condition{Field: col, Op: query.OpIn, Value: items}
}

// Like builds a LIKE predicate (raw pattern, e.g. "%alice%").
func Like[B any](base *B, fieldPtr *string, pattern string) Condition {
	_, col := resolveOrPanic(base, fieldPtr)
	return Condition{Field: col, Op: query.OpLike, Value: pattern}
}

// Contains builds a LIKE %val% predicate.
func Contains[B any](base *B, fieldPtr *string, val string) Condition {
	_, col := resolveOrPanic(base, fieldPtr)
	return Condition{Field: col, Op: query.OpLike, Value: "%" + val + "%"}
}

// IsNil builds an IS NULL predicate.
func IsNil[B any, F any](base *B, fieldPtr *F) Condition {
	_, col := resolveOrPanic(base, fieldPtr)
	return Condition{Field: col, Op: query.OpIsNil, Value: nil}
}

// --- internal: zero-allocation hot path via unsafe ---

func cond[B any, F any](base *B, fieldPtr *F, op query.Op, val F) Condition {
	_, col := resolveOrPanic(base, fieldPtr)
	return Condition{Field: col, Op: op, Value: val}
}

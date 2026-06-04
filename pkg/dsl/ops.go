package dsl

import (
	"reflect"
	"unsafe"

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
	fi, tm := resolveWithTypeMap(base, fieldPtr)
	items := make([]any, len(vals))
	for i, v := range vals {
		items[i] = v
	}
	return Condition{Field: fi.Column, Op: query.OpIn, Value: items, Table: tm.table}
}

// NotIn builds a NOT IN predicate. Mirrors In() with negated semantics.
func NotIn[B any, F any](base *B, fieldPtr *F, vals ...F) Condition {
	fi, tm := resolveWithTypeMap(base, fieldPtr)
	items := make([]any, len(vals))
	for i, v := range vals {
		items[i] = v
	}
	return Condition{Field: fi.Column, Op: query.OpNotIn, Value: items, Table: tm.table}
}

// InSub builds a "field IN (subquery)" predicate. The subquery is a built
// query.Query and must project exactly one column via its Select.
//
//	orders := query.From("orders").Select("user_id").
//	    Filter(dsl.Gt(o, &o.Total, 100)).Build()
//	db.Set(&users).Where(dsl.InSub(u, &u.ID, orders)).All()
func InSub[B any, F any](base *B, fieldPtr *F, sub query.Query) query.Node {
	fi := resolve(base, fieldPtr)
	return query.Subquery{Field: fi.Column, Op: query.OpIn, Query: sub}
}

// NotInSub builds a "field NOT IN (subquery)" predicate. Note that NOT IN
// against a subquery yielding any NULL matches no rows, per SQL three-valued
// logic.
func NotInSub[B any, F any](base *B, fieldPtr *F, sub query.Query) query.Node {
	fi := resolve(base, fieldPtr)
	return query.Subquery{Field: fi.Column, Op: query.OpNotIn, Query: sub}
}

// Exists builds an "EXISTS (subquery)" predicate. The subquery's WHERE may
// correlate to the outer table via a column-ref predicate (dsl.JoinEq).
func Exists(sub query.Query) query.Node {
	return query.Subquery{Op: query.OpExists, Query: sub}
}

// NotExists builds a "NOT EXISTS (subquery)" predicate.
func NotExists(sub query.Query) query.Node {
	return query.Subquery{Op: query.OpNotExists, Query: sub}
}

// Like builds a LIKE predicate (raw pattern, e.g. "%alice%").
func Like[B any](base *B, fieldPtr *string, pattern string) Condition {
	fi := resolve(base, fieldPtr)
	return Condition{Field: fi.Column, Op: query.OpLike, Value: pattern}
}

// Contains builds a LIKE %val% predicate.
func Contains[B any](base *B, fieldPtr *string, val string) Condition {
	fi := resolve(base, fieldPtr)
	return Condition{Field: fi.Column, Op: query.OpLike, Value: "%" + val + "%"}
}

// Set builds a column assignment for a bulk Update, resolving the column from
// the field pointer.
//
//	u := &User{}
//	db.Set(&User{}).
//	    Where(dsl.Eq(u, &u.Active, false)).
//	    Update(dsl.Set(u, &u.Active, true))
func Set[B any, F any](base *B, fieldPtr *F, val F) query.Assignment {
	fi := resolve(base, fieldPtr)
	return query.Assignment{Field: fi.Column, Value: val}
}

// IsNil builds an IS NULL predicate.
func IsNil[B any, F any](base *B, fieldPtr *F) Condition {
	fi := resolve(base, fieldPtr)
	return Condition{Field: fi.Column, Op: query.OpIsNil, Value: nil}
}

// IsNotNil builds an IS NOT NULL predicate.
func IsNotNil[B any, F any](base *B, fieldPtr *F) Condition {
	fi := resolve(base, fieldPtr)
	return Condition{Field: fi.Column, Op: query.OpIsNotNil, Value: nil}
}

// --- internal: zero-allocation hot path via unsafe ---

func cond[B any, F any](base *B, fieldPtr *F, op query.Op, val F) Condition {
	fi, tm := resolveWithTypeMap(base, fieldPtr)
	return Condition{Field: fi.Column, Op: op, Value: val, Table: tm.table}
}

func resolve[B any, F any](base *B, fieldPtr *F) *fieldInfo {
	fi, _ := resolveWithTypeMap(base, fieldPtr)
	return fi
}

func resolveWithTypeMap[B any, F any](base *B, fieldPtr *F) (*fieldInfo, *typeMap) {
	baseAddr := uintptr(unsafe.Pointer(base))
	fieldAddr := uintptr(unsafe.Pointer(fieldPtr))
	offset := fieldAddr - baseAddr

	tm := lookup(reflect.TypeOf(base).Elem())
	if tm == nil {
		panic("dsl: type not registered — call dsl.Register first")
	}
	fi, ok := tm.byOff[offset]
	if !ok {
		panic("dsl: invalid field pointer")
	}
	return fi, tm
}

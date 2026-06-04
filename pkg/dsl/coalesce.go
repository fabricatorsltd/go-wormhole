package dsl

import "github.com/fabricatorsltd/go-wormhole/pkg/query"

// Coalesce builds a COALESCE(arg1, arg2, ...) expression returning the first
// non-null operand. Operands are columns (dsl.Col) or literals (dsl.Lit), in
// order. Usable with SelectCoalesce, OrderByCoalesce, and as a WHERE operand.
//
//	u := &User{}
//	expr := dsl.Coalesce(dsl.Col(u, &u.Nickname), dsl.Col(u, &u.Name), dsl.Lit("Anonymous"))
//	db.Set(&rows).From("user").SelectCoalesce(expr, "display").All()
func Coalesce(args ...query.CoalesceArg) query.CoalesceExpr {
	return query.CoalesceExpr{Args: args}
}

// Col makes a column operand for Coalesce from a field pointer, type-safely.
func Col[B any, F any](base *B, fieldPtr *F) query.CoalesceArg {
	fi := resolve(base, fieldPtr)
	return query.CoalesceArg{Column: fi.Column}
}

// Lit makes a literal operand for Coalesce; the value is emitted as a bound
// parameter.
func Lit(v any) query.CoalesceArg {
	return query.CoalesceArg{Value: v}
}

// CoalesceEq builds a WHERE predicate comparing a COALESCE expression to a value:
// COALESCE(...) = val. See also CoalesceNeq/Gt/Lt.
//
//	db.Set(&rows).Where(dsl.CoalesceEq(dsl.Coalesce(dsl.Col(u, &u.Deleted), dsl.Lit(false)), false)).All()
func CoalesceEq(expr query.CoalesceExpr, val any) Condition {
	return coalesceCond(expr, query.OpEq, val)
}

// CoalesceNeq builds a COALESCE(...) != val predicate.
func CoalesceNeq(expr query.CoalesceExpr, val any) Condition {
	return coalesceCond(expr, query.OpNeq, val)
}

// CoalesceGt builds a COALESCE(...) > val predicate.
func CoalesceGt(expr query.CoalesceExpr, val any) Condition {
	return coalesceCond(expr, query.OpGt, val)
}

// CoalesceLt builds a COALESCE(...) < val predicate.
func CoalesceLt(expr query.CoalesceExpr, val any) Condition {
	return coalesceCond(expr, query.OpLt, val)
}

func coalesceCond(expr query.CoalesceExpr, op query.Op, val any) Condition {
	return Condition{Coalesce: &expr, Op: op, Value: val}
}

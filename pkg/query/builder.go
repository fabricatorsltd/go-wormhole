package query

// Builder provides a fluent API to compose a provider-neutral Query AST.
type Builder struct {
	q           Query
	predicates  []Node
	havingPreds []Node
}

// From starts a new query targeting the given entity name.
func From(entity string) *Builder {
	return &Builder{q: Query{EntityName: entity}}
}

// Where adds a comparison predicate (raw form).
func (b *Builder) Where(field string, op Op, value any) *Builder {
	b.predicates = append(b.predicates, Predicate{Field: field, Op: op, Value: value})
	return b
}

// Filter adds one or more typed predicates produced by the dsl package.
// Accepts any query.Node, so callers can mix raw Predicates with composite
// dsl.And / dsl.Or trees.
func (b *Builder) Filter(nodes ...Node) *Builder {
	b.predicates = append(b.predicates, nodes...)
	return b
}

// And combines the current predicates with AND logic.
func (b *Builder) And(fn func(b *Builder)) *Builder {
	sub := &Builder{}
	fn(sub)
	b.predicates = append(b.predicates, Composite{Logic: LogicAnd, Children: sub.predicates})
	return b
}

// Or combines the current predicates with OR logic.
func (b *Builder) Or(fn func(b *Builder)) *Builder {
	sub := &Builder{}
	fn(sub)
	b.predicates = append(b.predicates, Composite{Logic: LogicOr, Children: sub.predicates})
	return b
}

// OrderBy appends a sort clause.
func (b *Builder) OrderBy(field string, dir SortDir) *Builder {
	b.q.OrderBy = append(b.q.OrderBy, Sort{Field: field, Dir: dir})
	return b
}

// OrderByCase appends a sort clause that uses a CASE WHEN expression
// instead of a column reference. Useful for sort-priority patterns where
// some rows should come first (e.g. "currently-pinned items at the top").
func (b *Builder) OrderByCase(c CaseExpr, dir SortDir) *Builder {
	b.q.OrderBy = append(b.q.OrderBy, Sort{Case: &c, Dir: dir})
	return b
}

// OrderByDistance appends a pgvector nearest-neighbor sort; combine with Limit
// for top-k. PostgreSQL only.
func (b *Builder) OrderByDistance(d VectorDistance, dir SortDir) *Builder {
	b.q.OrderBy = append(b.q.OrderBy, Sort{Distance: &d, Dir: dir})
	return b
}

// OrderByCoalesce appends a sort on a COALESCE expression (e.g. order by a
// fallback value when the primary column is NULL).
func (b *Builder) OrderByCoalesce(e CoalesceExpr, dir SortDir) *Builder {
	b.q.OrderBy = append(b.q.OrderBy, Sort{Coalesce: &e, Dir: dir})
	return b
}

// Join attaches an INNER JOIN <entity> ON <on> clause to the query.
// The on predicate is typically produced by dsl.JoinEq for type-safety, but
// any Predicate or Composite is accepted.
func (b *Builder) Join(entity string, on Node) *Builder {
	b.q.Joins = append(b.q.Joins, JoinSpec{Type: JoinInner, Entity: entity, On: on})
	return b
}

// LeftJoin attaches a LEFT JOIN <entity> ON <on> clause to the query.
func (b *Builder) LeftJoin(entity string, on Node) *Builder {
	b.q.Joins = append(b.q.Joins, JoinSpec{Type: JoinLeft, Entity: entity, On: on})
	return b
}

// RightJoin attaches a RIGHT JOIN <entity> ON <on> clause to the query.
func (b *Builder) RightJoin(entity string, on Node) *Builder {
	b.q.Joins = append(b.q.Joins, JoinSpec{Type: JoinRight, Entity: entity, On: on})
	return b
}

// FullJoin attaches a FULL JOIN <entity> ON <on> clause to the query.
func (b *Builder) FullJoin(entity string, on Node) *Builder {
	b.q.Joins = append(b.q.Joins, JoinSpec{Type: JoinFull, Entity: entity, On: on})
	return b
}

// Distinct emits SELECT DISTINCT, collapsing duplicate result rows.
func (b *Builder) Distinct() *Builder {
	b.q.Distinct = true
	return b
}

// Select restricts the SELECT list to the given columns (field or column
// names). With no columns the query selects every mapped field.
func (b *Builder) Select(columns ...string) *Builder {
	b.q.Columns = append(b.q.Columns, columns...)
	return b
}

// Union combines the query with another via UNION (duplicate rows removed).
func (b *Builder) Union(other Query) *Builder {
	b.q.SetOps = append(b.q.SetOps, SetOp{Kind: SetUnion, Query: other})
	return b
}

// UnionAll combines the query with another via UNION ALL (duplicates kept).
func (b *Builder) UnionAll(other Query) *Builder {
	b.q.SetOps = append(b.q.SetOps, SetOp{Kind: SetUnionAll, Query: other})
	return b
}

// Intersect keeps only rows present in both queries.
func (b *Builder) Intersect(other Query) *Builder {
	b.q.SetOps = append(b.q.SetOps, SetOp{Kind: SetIntersect, Query: other})
	return b
}

// Except keeps rows in this query that are not in the other.
func (b *Builder) Except(other Query) *Builder {
	b.q.SetOps = append(b.q.SetOps, SetOp{Kind: SetExcept, Query: other})
	return b
}

// SelectCase adds a CASE expression to the SELECT list under an alias.
func (b *Builder) SelectCase(expr CaseExpr, alias string) *Builder {
	b.q.CaseSelects = append(b.q.CaseSelects, CaseSelect{Expr: expr, Alias: alias})
	return b
}

// SelectCoalesce projects a COALESCE expression into the SELECT list under an
// alias, scanned into the destination field mapped to that column name.
func (b *Builder) SelectCoalesce(expr CoalesceExpr, alias string) *Builder {
	b.q.CoalesceSelects = append(b.q.CoalesceSelects, CoalesceSelect{Expr: expr, Alias: alias})
	return b
}

// Limit sets the maximum number of results.
func (b *Builder) Limit(n int) *Builder {
	b.q.Limit = n
	return b
}

// Offset sets the number of results to skip.
func (b *Builder) Offset(n int) *Builder {
	b.q.Offset = n
	return b
}

// Include requests eager loading of a relation.
func (b *Builder) Include(relation string) *Builder {
	b.q.Includes = append(b.q.Includes, relation)
	return b
}

// GroupBy appends fields to the GROUP BY clause.
func (b *Builder) GroupBy(fields ...string) *Builder {
	b.q.GroupBy = append(b.q.GroupBy, fields...)
	return b
}

// Having adds one or more predicates for the HAVING clause (AND logic).
// Accepts query.Node so composite dsl.And / dsl.Or trees are supported.
func (b *Builder) Having(nodes ...Node) *Builder {
	b.havingPreds = append(b.havingPreds, nodes...)
	return b
}

// Aggregate appends an aggregate expression (COUNT, SUM, AVG, MIN, MAX)
// to the SELECT clause. field may be "*" or empty for COUNT(*).
func (b *Builder) Aggregate(fn AggFunc, field, alias string) *Builder {
	b.q.Aggregates = append(b.q.Aggregates, Aggregate{Func: fn, Field: field, Alias: alias})
	return b
}

// Build finalises and returns the immutable Query AST.
func (b *Builder) Build() Query {
	if len(b.predicates) == 1 {
		b.q.Where = b.predicates[0]
	} else if len(b.predicates) > 1 {
		b.q.Where = Composite{Logic: LogicAnd, Children: b.predicates}
	}
	if len(b.havingPreds) == 1 {
		b.q.Having = b.havingPreds[0]
	} else if len(b.havingPreds) > 1 {
		b.q.Having = Composite{Logic: LogicAnd, Children: b.havingPreds}
	}
	return b.q
}

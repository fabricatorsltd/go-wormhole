package query

// Builder provides a fluent API to compose a provider-neutral Query AST.
type Builder struct {
	q          Query
	predicates []Node
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

// Filter adds one or more typed predicates produced by generated field
// builders (e.g. UserFields.Age.Gt(18)).
func (b *Builder) Filter(preds ...Predicate) *Builder {
	for _, p := range preds {
		b.predicates = append(b.predicates, p)
	}
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

// Build finalises and returns the immutable Query AST.
func (b *Builder) Build() Query {
	if len(b.predicates) == 1 {
		b.q.Where = b.predicates[0]
	} else if len(b.predicates) > 1 {
		b.q.Where = Composite{Logic: LogicAnd, Children: b.predicates}
	}
	return b.q
}

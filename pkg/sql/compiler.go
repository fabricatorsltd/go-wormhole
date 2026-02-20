package sql

import (
	"fmt"
	"strings"

	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
)

// Compiled holds a parameterized SQL statement ready for execution.
type Compiled struct {
	SQL    string
	Params []any
}

// Compiler translates query AST nodes into parameterized SQL.
type Compiler struct {
	// Placeholder style: "?" (mysql/sqlite) or "$N" (postgres).
	// Default is "?".
	Numbered bool
}

// Select compiles a query.Query into a SELECT statement.
func (c *Compiler) Select(meta *model.EntityMeta, q query.Query) Compiled {
	var b strings.Builder
	var params []any

	// SELECT columns
	b.WriteString("SELECT ")
	for i, f := range meta.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(f.Column))
	}

	// FROM
	b.WriteString(" FROM ")
	b.WriteString(quoteIdent(meta.Name))

	// WHERE
	if q.Where != nil {
		b.WriteString(" WHERE ")
		c.compileNode(&b, &params, q.Where)
	}

	// ORDER BY
	for i, s := range q.OrderBy {
		if i == 0 {
			b.WriteString(" ORDER BY ")
		} else {
			b.WriteString(", ")
		}
		col := fieldColumn(meta, s.Field)
		b.WriteString(quoteIdent(col))
		if s.Dir == query.Desc {
			b.WriteString(" DESC")
		} else {
			b.WriteString(" ASC")
		}
	}

	// LIMIT / OFFSET
	if q.Limit > 0 {
		b.WriteString(fmt.Sprintf(" LIMIT %d", q.Limit))
	}
	if q.Offset > 0 {
		b.WriteString(fmt.Sprintf(" OFFSET %d", q.Offset))
	}

	return Compiled{SQL: b.String(), Params: params}
}

// Insert compiles an INSERT statement for all fields of an entity.
func (c *Compiler) Insert(meta *model.EntityMeta, values map[string]any) Compiled {
	var cols, placeholders []string
	var params []any
	idx := 1

	for _, f := range meta.Fields {
		if f.AutoIncr {
			continue
		}
		cols = append(cols, quoteIdent(f.Column))
		params = append(params, values[f.FieldName])
		placeholders = append(placeholders, c.ph(idx))
		idx++
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(meta.Name),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	return Compiled{SQL: sql, Params: params}
}

// Update compiles a partial UPDATE for only the specified changed fields.
func (c *Compiler) Update(meta *model.EntityMeta, values map[string]any, changed []string, pkValue any) Compiled {
	var sets []string
	var params []any
	idx := 1

	changedSet := make(map[string]struct{}, len(changed))
	for _, ch := range changed {
		changedSet[ch] = struct{}{}
	}

	for _, f := range meta.Fields {
		if _, ok := changedSet[f.FieldName]; !ok {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = %s", quoteIdent(f.Column), c.ph(idx)))
		params = append(params, values[f.FieldName])
		idx++
	}

	if len(sets) == 0 {
		return Compiled{}
	}

	pkCol := "id"
	if meta.PrimaryKey != nil {
		pkCol = meta.PrimaryKey.Column
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
		quoteIdent(meta.Name),
		strings.Join(sets, ", "),
		quoteIdent(pkCol),
		c.ph(idx),
	)
	params = append(params, pkValue)
	return Compiled{SQL: sql, Params: params}
}

// Delete compiles a DELETE by primary key.
func (c *Compiler) Delete(meta *model.EntityMeta, pkValue any) Compiled {
	pkCol := "id"
	if meta.PrimaryKey != nil {
		pkCol = meta.PrimaryKey.Column
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		quoteIdent(meta.Name),
		quoteIdent(pkCol),
		c.ph(1),
	)
	return Compiled{SQL: sql, Params: []any{pkValue}}
}

// FindByPK compiles a SELECT … WHERE pk = ? for a single entity.
func (c *Compiler) FindByPK(meta *model.EntityMeta, pkValue any) Compiled {
	var cols []string
	for _, f := range meta.Fields {
		cols = append(cols, quoteIdent(f.Column))
	}

	pkCol := "id"
	if meta.PrimaryKey != nil {
		pkCol = meta.PrimaryKey.Column
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s LIMIT 1",
		strings.Join(cols, ", "),
		quoteIdent(meta.Name),
		quoteIdent(pkCol),
		c.ph(1),
	)
	return Compiled{SQL: sql, Params: []any{pkValue}}
}

// --- Join support ---

// SelectWithJoins compiles a query with LEFT JOIN clauses for eager-loaded relations.
func (c *Compiler) SelectWithJoins(meta *model.EntityMeta, q query.Query, joins []JoinSpec) Compiled {
	compiled := c.Select(meta, q)
	if len(joins) == 0 {
		return compiled
	}

	// Inject JOINs right after FROM table
	fromClause := fmt.Sprintf("FROM %s", quoteIdent(meta.Name))
	var joinSQL strings.Builder
	joinSQL.WriteString(fromClause)
	for _, j := range joins {
		joinSQL.WriteString(fmt.Sprintf(" LEFT JOIN %s ON %s.%s = %s.%s",
			quoteIdent(j.Table),
			quoteIdent(meta.Name), quoteIdent(j.LocalKey),
			quoteIdent(j.Table), quoteIdent(j.ForeignKey),
		))
	}

	compiled.SQL = strings.Replace(compiled.SQL, fromClause, joinSQL.String(), 1)
	return compiled
}

// JoinSpec describes a LEFT JOIN relationship.
type JoinSpec struct {
	Table      string // related table name
	LocalKey   string // column on the source table
	ForeignKey string // column on the related table
}

// --- internal ---

func (c *Compiler) compileNode(b *strings.Builder, params *[]any, node query.Node) {
	switch n := node.(type) {
	case query.Predicate:
		c.compilePredicate(b, params, n)
	case query.Composite:
		c.compileComposite(b, params, n)
	}
}

func (c *Compiler) compilePredicate(b *strings.Builder, params *[]any, p query.Predicate) {
	col := p.Field // use field name as column (schema resolves later)
	b.WriteString(quoteIdent(col))

	switch p.Op {
	case query.OpEq:
		b.WriteString(" = ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpNeq:
		b.WriteString(" != ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpGt:
		b.WriteString(" > ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpGte:
		b.WriteString(" >= ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLt:
		b.WriteString(" < ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLte:
		b.WriteString(" <= ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLike:
		b.WriteString(" LIKE ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpIsNil:
		b.WriteString(" IS NULL")
	case query.OpIn:
		items, ok := p.Value.([]any)
		if !ok {
			b.WriteString(" IN ()")
			return
		}
		b.WriteString(" IN (")
		for i, item := range items {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(c.ph(len(*params) + 1))
			*params = append(*params, item)
		}
		b.WriteString(")")
	}
}

func (c *Compiler) compileComposite(b *strings.Builder, params *[]any, comp query.Composite) {
	op := " AND "
	if comp.Logic == query.LogicOr {
		op = " OR "
	}

	b.WriteString("(")
	for i, child := range comp.Children {
		if i > 0 {
			b.WriteString(op)
		}
		c.compileNode(b, params, child)
	}
	b.WriteString(")")
}

func (c *Compiler) ph(n int) string {
	if c.Numbered {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func quoteIdent(s string) string {
	return `"` + s + `"`
}

func fieldColumn(meta *model.EntityMeta, fieldName string) string {
	if f := meta.Field(fieldName); f != nil {
		return f.Column
	}
	return fieldName
}

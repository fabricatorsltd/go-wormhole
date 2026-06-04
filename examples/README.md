# Examples

Runnable, self-contained programs. Each opens an in-memory SQLite database,
creates its schema, and exercises one area of the API. Run any of them with:

```bash
go run ./examples/crud
go run ./examples/querying
go run ./examples/advanced
go run ./examples/modeling
go run ./examples/relationships
go run ./examples/unitofwork
```

| Example                          | Shows                                                                 |
|----------------------------------|-----------------------------------------------------------------------|
| [`crud`](crud)                   | Insert, find, in-memory mutation with partial UPDATE, delete          |
| [`querying`](querying)           | DSL predicates, ordering, pagination, joins, grouped aggregates       |
| [`advanced`](advanced)           | DISTINCT projections, subquery filters, set operations, CASE          |
| [`modeling`](modeling)           | Composite keys, computed columns, JSON value objects, single-table hierarchy |
| [`relationships`](relationships) | 1:N, 1:1, belongs-to navigation fields and `Include` eager loading    |
| [`unitofwork`](unitofwork)       | Query filters (multi-tenant / soft-delete), tracking, streaming, transactions |

The programs use SQLite so they run with no external services. The ORM calls
(`Add`/`Save`/`Set`/the query DSL) carry over to PostgreSQL, MySQL, and SQL
Server by swapping the registered provider; the inline `CREATE TABLE` DDL and a
few SQL-only shapes here (generated columns, CASE) are dialect-specific. See
[docs/05-providers.md](../docs/05-providers.md).

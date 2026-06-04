# Entity Relationships

`go-wormhole` models relationships through Go struct tags and navigation
fields (pointers and slices of pointers). It supports **1:1**, **1:N**, and
**N:M** declarations, generates `FOREIGN KEY` constraints in migrations, and
eager-loads related rows with `Include`.

Current scope:

- **1:1, 1:N, belongs-to**: parsed, foreign keys generated, eager loading works.
- **N:M**: parsed, foreign-key aware, and eager loading via `Include` works
  (loaded through the join table in a second batched query).
- **Cascade delete** is not yet implemented. Foreign-key fix-up and parent-before-child
  insert ordering on `Save` are.

## Defining Relationships

A navigation field is any `*Other` or `[]*Other` field, where `Other` is
another entity struct. Navigation fields are not stored as columns. The `db`
tag uses `;`-separated pairs with `:` key/value separators, the same grammar as
scalar columns (`db:"column:id;primary_key;auto_increment"`).

Relationship attributes:

- `fk:<column>`: the foreign-key column on the *child* table.
- `ref:<column>`: the key column on the *owning* table (defaults to its primary key).
- `join:<table>`: the join table for a many-to-many relationship.

### One-to-Many (1:N)

The most common relationship. Use a slice of pointers for the collection.

```go
type User struct {
    ID     int      `db:"column:id;primary_key;auto_increment"`
    Name   string   `db:"column:name"`
    Orders []*Order `db:"fk:user_id"` // one user has many orders
}

type Order struct {
    ID     int     `db:"column:id;primary_key;auto_increment"`
    UserID int     `db:"column:user_id"` // foreign key on the child table
    Total  float64 `db:"column:total"`
}
```

If you omit `fk:`, the foreign-key column defaults to the snake_case owner type
name plus `_id` (e.g. `User` → `user_id`).

### One-to-One (1:1)

A single pointer. When the owning struct does **not** carry the foreign key, the
key lives on the related table (has-one):

```go
type User struct {
    ID      int      `db:"column:id;primary_key;auto_increment"`
    Profile *Profile `db:"fk:user_id"` // FK user_id lives on profile
}

type Profile struct {
    ID     int    `db:"column:id;primary_key;auto_increment"`
    UserID int    `db:"column:user_id"`
    Bio    string `db:"column:bio"`
}
```

### Belongs-To

When the owning struct carries the foreign key (a `<Nav>ID` or `<Target>ID`
column), the pointer is a belongs-to back-reference:

```go
type Order struct {
    ID     int    `db:"column:id;primary_key;auto_increment"`
    UserID int    `db:"column:user_id"` // FK on this table
    User   *User  `db:"ref"`            // belongs-to User via user_id
}
```

### Many-to-Many (N:M)

N:M relationships use a join table named with `join:`:

```go
type Student struct {
    ID      int       `db:"column:id;primary_key;auto_increment"`
    Courses []*Course `db:"join:enrollments;ref:student_id;fk:course_id"`
}

type Course struct {
    ID       int        `db:"column:id;primary_key;auto_increment"`
    Students []*Student `db:"join:enrollments;ref:course_id;fk:student_id"`
}
```

For N:M, `ref:` and `fk:` name the owner and target columns *in the join table*.
Defaults are the snake_case type names plus `_id`. `Include` loads N:M relations
through the join table (one query for the link rows, one for the targets).

## Eager Loading with `Include`

By default, relationships are not loaded. `Include` loads them with one batched
`WHERE key IN (...)` query per relation (no cartesian JOIN):

```go
u := &User{}

var users []*User
err := db.Set(&users).
    Include("Orders").  // load all orders for each user
    Include("Profile").
    Where(dsl.Eq(u, &u.Name, "Alice")).
    All()

// users[0].Orders and users[0].Profile are now populated.
```

`Include` takes the Go navigation field name. For a compile-time-checked name,
resolve it through the pointer DSL:

```go
db.Set(&users).Include(dsl.FieldName(u, &u.Orders)).All()
```

## Foreign Key Constraints

The migrations engine emits a column-level `FOREIGN KEY` reference for each
relationship when it creates a table or adds a foreign-key column:

```sql
CREATE TABLE "order" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "user_id" INTEGER NOT NULL REFERENCES "user" ("id"),
  "total" REAL NOT NULL DEFAULT 0
);
```

> SQLite does not enforce foreign keys unless `PRAGMA foreign_keys = ON` is set
> on the connection. PostgreSQL, MySQL, and SQL Server enforce them by default.

## Saving Object Graphs

When you `Add` a new parent and its new children in any order, `Save` orders the
inserts parent-first and writes each generated primary key into the children's
foreign-key columns before they are inserted:

```go
user := &User{Name: "Alice"}
o1 := &Order{Total: 10, User: user}
user.Orders = []*Order{o1}

db.Add(o1, user) // order added before its parent; still works
db.Save()        // user inserted first; o1.UserID set to user.ID
```

A dependency cycle among new rows (two entities that each belong to the other)
is reported as an error rather than ordered arbitrarily.

## Conventions vs. Explicit Configuration

1. **Primary key**: mark it with `primary_key` (e.g. `db:"column:id;primary_key"`).
2. **Implicit FK**: a `<Nav>ID` / `<Target>ID` field makes a pointer a belongs-to;
   a collection's FK defaults to `<owner_type>_id`.
3. **Explicit keys**: use `fk:` / `ref:` when names do not follow the conventions.

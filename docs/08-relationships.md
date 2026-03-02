# Entity Relationships

`go-wormhole` manages database relationships through Go struct tags and pointers. It supports **1:1**, **1:N**, and **N:M** relationships with automatic JOIN generation and eager loading.

## Defining Relationships

Relationships are defined using the `db` tag with the `ref` (reference) and `fk` (foreign key) attributes.

### One-to-One (1:1)

In a 1:1 relationship, one entity points to exactly one instance of another entity.

```go
type User struct {
    ID      int      `db:"id,pk"`
    Name    string   `db:"name"`
    Profile *Profile `db:"ref=ID"` // Parent: references Profile where Profile.UserID == User.ID
}

type Profile struct {
    ID     int    `db:"id,pk"`
    Bio    string `db:"bio"`
    UserID int    `db:"user_id"` // The foreign key
    User   *User  `db:"ref=user_id"` // Child: references User where User.ID == Profile.UserID
}
```

- **`ref=ID`** on `User.Profile` tells the engine to look for a `Profile` where its foreign key matches `User.ID`.
- **`ref=user_id`** on `Profile.User` tells the engine to look for a `User` where `User.ID` matches `Profile.UserID`.

### One-to-Many (1:N)

The most common relationship. Use a slice of pointers to represent the collection.

```go
type User struct {
    ID     int      `db:"id,pk"`
    Orders []*Order `db:"ref=ID"` // One user has many orders
}

type Order struct {
    ID     int     `db:"id,pk"`
    Total  float64 `db:"total"`
    UserID int     `db:"user_id"` // Foreign key in the child table
}
```

Wormhole automatically detects that `Order` belongs to `User` by looking for a field that matches the `User` type or a naming convention (e.g., `UserID`).

### Many-to-Many (N:M)

N:M relationships require a join table. Define the join table using the `join` attribute.

```go
type Student struct {
    ID      int       `db:"id,pk"`
    Courses []*Course `db:"join=enrollments,ref=student_id"`
}

type Course struct {
    ID       int        `db:"id,pk"`
    Students []*Student `db:"join=enrollments,ref=course_id"`
}

// The join table (enrollments)
type Enrollment struct {
    StudentID int `db:"student_id,pk"`
    CourseID  int `db:"course_id,pk"`
}
```

## Eager Loading with `Include`

By default, relationships are **not loaded** to save resources. Use the `.Include()` method in the fluent API to load them.

```go
u := &User{}

users, err := db.Set(&User{}).
    Include(&u.Orders). // Load all orders for each user
    Where(dsl.Eq(&u, &u.Name, "Alice")).
    All()

// Now users[0].Orders is populated
fmt.Println(len(users[0].Orders))
```

### Nested Includes

You can chain includes to load deep relationship trees:

```go
users, err := db.Set(&User{}).
    Include(&u.Orders).
    Include(&u.Profile).
    All()
```

## Foreign Key Constraints

When using the **Migrations Engine**, `go-wormhole` uses these metadata to generate `FOREIGN KEY` constraints in your SQL schema:

```sql
ALTER TABLE "orders" 
ADD CONSTRAINT "fk_orders_user" 
FOREIGN KEY ("user_id") REFERENCES "users" ("id");
```

## Conventions vs. Explicit Configuration

1. **Explicit PK**: Always mark your primary key with `pk` (e.g., `db:"id,pk"`).
2. **Implicit FK**: If a field is named `EntityID` (e.g., `UserID`), it's automatically treated as a foreign key to `User`.
3. **Explicit Ref**: Use `ref=field_name` when the naming doesn't follow conventions or for 1:1 "back-references".

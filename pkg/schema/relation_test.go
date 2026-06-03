package schema

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

type relUser struct {
	ID      int          `db:"column:id;primary_key;auto_increment"`
	Name    string       `db:"column:name"`
	Orders  []*relOrder  `db:"ref"`           // 1:N, FK on order
	Profile *relProfile  `db:"ref"`           // 1:1, FK on profile
	Courses []*relCourse `db:"join:enrollments"` // N:M
}

type relOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
}

type relProfile struct {
	ID     int    `db:"column:id;primary_key;auto_increment"`
	UserID int    `db:"column:user_id"`
	Bio    string `db:"column:bio"`
}

type relCourse struct {
	ID    int    `db:"column:id;primary_key;auto_increment"`
	Title string `db:"column:title"`
}

type relOrderWithOwner struct {
	ID     int      `db:"column:id;primary_key;auto_increment"`
	UserID int      `db:"column:user_id"`
	User   *relUser `db:"ref"` // BelongsTo: FK (user_id) on this table
}

func TestParse_Relations(t *testing.T) {
	meta := Parse(&relUser{})

	// Navigation fields must not become columns.
	for _, f := range meta.Fields {
		if f.FieldName == "Orders" || f.FieldName == "Profile" || f.FieldName == "Courses" {
			t.Fatalf("navigation field %q leaked into columns", f.FieldName)
		}
	}
	if got := len(meta.Relations); got != 3 {
		t.Fatalf("relations: got %d, want 3", got)
	}

	orders := meta.Relation("Orders")
	if orders == nil || orders.Kind != model.RelationOneToMany {
		t.Fatalf("Orders: want OneToMany, got %+v", orders)
	}
	if orders.LocalKey != "id" || orders.ForeignKey != "rel_user_id" {
		t.Errorf("Orders keys: got local=%q fk=%q, want id/rel_user_id", orders.LocalKey, orders.ForeignKey)
	}
	if orders.TargetEntity != "rel_order" {
		t.Errorf("Orders target: got %q, want rel_order", orders.TargetEntity)
	}

	profile := meta.Relation("Profile")
	if profile == nil || profile.Kind != model.RelationOneToOne {
		t.Fatalf("Profile: want OneToOne, got %+v", profile)
	}
	if profile.ForeignKey != "rel_user_id" {
		t.Errorf("Profile fk: got %q, want rel_user_id", profile.ForeignKey)
	}

	courses := meta.Relation("Courses")
	if courses == nil || courses.Kind != model.RelationManyToMany {
		t.Fatalf("Courses: want ManyToMany, got %+v", courses)
	}
	if courses.JoinTable != "enrollments" {
		t.Errorf("Courses join table: got %q, want enrollments", courses.JoinTable)
	}
	if courses.JoinLocalKey != "rel_user_id" || courses.JoinForeignKey != "rel_course_id" {
		t.Errorf("Courses join keys: got %q/%q, want rel_user_id/rel_course_id",
			courses.JoinLocalKey, courses.JoinForeignKey)
	}
}

type jsonNavEntity struct {
	ID    int       `db:"column:id;primary_key;auto_increment"`
	Leafs []*relLeaf `db:"column:leafs;json"` // JSON column, NOT a relation
	Owner *relLeaf   `db:"column:owner;json"` // JSON column, NOT a relation
}

type relLeaf struct {
	Hash string
}

// A json-tagged pointer/slice-of-pointer field is a JSON column, not a
// navigation relation: the json directive wins over relation detection.
func TestParse_JSONTagBeatsNavigation(t *testing.T) {
	meta := Parse(&jsonNavEntity{})

	if len(meta.Relations) != 0 {
		t.Fatalf("json fields leaked into relations: %+v", meta.Relations)
	}
	for _, name := range []string{"Leafs", "Owner"} {
		f := meta.Field(name)
		if f == nil {
			t.Fatalf("%s missing from columns", name)
		}
		if f.Tags["json"] != "true" {
			t.Errorf("%s: json tag not recorded (%v)", name, f.Tags)
		}
	}
}

func TestParse_BelongsTo(t *testing.T) {
	meta := Parse(&relOrderWithOwner{})
	user := meta.Relation("User")
	if user == nil || user.Kind != model.RelationBelongsTo {
		t.Fatalf("User: want BelongsTo, got %+v", user)
	}
	if user.LocalKey != "user_id" {
		t.Errorf("User local key: got %q, want user_id", user.LocalKey)
	}
	if user.ForeignKey != "id" {
		t.Errorf("User fk (target pk): got %q, want id", user.ForeignKey)
	}
}

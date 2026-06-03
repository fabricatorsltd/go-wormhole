package migrations_test

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name   string
		script string
		want   []string
	}{
		{
			name:   "plain",
			script: "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);",
			want:   []string{"CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"},
		},
		{
			name:   "semicolon inside a comment does not leak a fragment",
			script: "-- drop; then recreate\nCREATE TABLE a (id INT);",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
		{
			name:   "header comment block",
			script: "-- generated\n-- dialect: pg\n\nCREATE TABLE a (id INT);",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
		{
			name:   "trailing empty statement dropped",
			script: "CREATE TABLE a (id INT);\n",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := migrations.SplitStatements(c.script)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v, want %#v", got, c.want)
			}
		})
	}
}

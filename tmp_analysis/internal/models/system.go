package models

type FeaturesToggle struct {
	Id     string `db:"primary_key; column:id"`
	UserId string `db:"primary_key; column:user_id"`
	Status int16  `db:"column:status"`
}

type Setting struct {
	Id           string `db:"primary_key; column:id"`
	Description  string `db:"column:description"`
	Category     string `db:"column:category"`
	Value        string `db:"column:value"`
	DefaultValue string `db:"column:default_value"`
}

package models

import (
	"time"
)

type EmailLog struct {
	Id        string    `db:"primary_key; auto_increment; column:id"`
	Status    int16     `db:"column:status"`
	CreatedAt time.Time `db:"column:created_at"`
	UserId    string    `db:"column:user_id"`
	Address   string    `db:"column:address"`
	Subject   string    `db:"column:subject"`
	Message   string    `db:"column:message"`
}

type NotificationLog struct {
	Id         string    `db:"primary_key; auto_increment; column:id"`
	Status     int16     `db:"column:status"`
	CreatedAt  time.Time `db:"column:created_at"`
	ExternalId string    `db:"column:external_id"`
	TargetType string    `db:"column:target_type"`
	Target     string    `db:"column:target"`
}

type SystemLog struct {
	Id        string    `db:"primary_key; auto_increment; column:id"`
	Status    int16     `db:"column:status"`
	CreatedAt time.Time `db:"column:created_at"`
	UserId    string    `db:"column:user_id"`
	Severity  int16     `db:"column:severity"`
	Source    string    `db:"column:source"`
	Message   string    `db:"column:message"`
}

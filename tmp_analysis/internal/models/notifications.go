package models

import (
	"time"
)

type Notification struct {
	Id         string    `db:"primary_key; auto_increment; column:id"`
	UserId     string    `db:"column:user_id"`
	CreatedAt  time.Time `db:"column:created_at"`
	Status     int16     `db:"column:status"`
	TargetType string    `db:"column:target_type"`
	Title      string    `db:"column:title"`
	Message    string    `db:"column:message"`
	Payload    string    `db:"column:payload"` // JSON string
}

type UserDevice struct {
	UserId   string `db:"column:user_id; index:idx_user_devices_user"`
	Platform string `db:"column:platform"`
	Token    string `db:"column:token; index:idx_user_devices_token"`
}

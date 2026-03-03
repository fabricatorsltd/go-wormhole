package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type Chat struct {
	Id           string    `db:"primary_key; auto_increment; column:id"`
	Participants string    `db:"column:participants"` // JSON
	CreatedAt    time.Time `db:"column:created_at"`
	LastActivity time.Time `db:"column:last_activity"`
}

type ChatReaction struct {
	Type    ReactionType `json:"type"`
	UserIds []string     `json:"user_ids"`
}

type ChatReactions []ChatReaction

type ChatMessage struct {
	Id        string        `db:"primary_key; auto_increment; column:id"`
	ChatId    string        `db:"column:chat_id"`
	SenderId  string        `db:"column:sender_id"`
	Reactions ChatReactions `db:"column:reactions"` // JSON Serialized
}

func (r *ChatReactions) Scan(value any) error {
	if value == nil {
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(b, r)
}

func (r ChatReactions) Value() (driver.Value, error) {
	return json.Marshal(r)
}

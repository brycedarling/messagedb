package messagedb

import (
	"time"

	"github.com/google/uuid"
)

// Messages ...
type Messages []*Message

// Message ...
type Message struct {
	ID              string
	StreamName      string
	Type            string
	Data            map[string]interface{}
	Metadata        map[string]interface{}
	ExpectedVersion *int
	Position        int
	GlobalPosition  int
	Time            time.Time
}

// NewMessage ...
func NewMessage(streamName, messageType string) *Message {
	return &Message{
		ID:         uuid.New().String(),
		StreamName: streamName,
		Type:       messageType,
	}
}

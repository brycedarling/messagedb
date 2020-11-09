package messagedb_test

import (
	"testing"

	"github.com/brycedarling/messagedb"
)

func TestNewMessage(t *testing.T) {
	streamName := "stream"
	messageType := "type"
	msg := messagedb.NewMessage(streamName, messageType)

	if msg.ID == "" {
		t.Errorf("expected message id")
	}
	if msg.StreamName != streamName {
		t.Errorf("got %s, want %s", msg.StreamName, streamName)
	}
	if msg.Type != messageType {
		t.Errorf("got %s, want %s", msg.Type, messageType)
	}
}

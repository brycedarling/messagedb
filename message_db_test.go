package messagedb_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/nurseybushc/messagedb"
	"github.com/google/uuid"
)

func TestCreateSubscription(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error '%s' when opening a stub database connection", err)
	}
	defer db.Close()

	streamName := "stream"
	messageType := "type"

	subscriberID := "test"
	subscriberStreamName := fmt.Sprintf("subscriberPosition-%s", subscriberID)

	columns := []string{"id", "name", "type", "position", "global_position", "data", "metadata", "time"}

	mock.ExpectQuery("get_last_stream_message").
		WithArgs(subscriberStreamName).
		WillReturnRows(mock.NewRows(columns).
			AddRow(uuid.New(), subscriberStreamName, "Read", 0, 0, nil, nil, time.Now()))

	mock.ExpectQuery("get_category_messages").
		WithArgs(streamName, 1, 100).
		WillReturnRows(mock.NewRows(columns).
			AddRow(uuid.New(), streamName, messageType, 0, 0, nil, nil, time.Now()))

	m := messagedb.New(db)

	sub, err := m.CreateSubscription(streamName, subscriberID)
	if err != nil {
		t.Fatalf("unexpected error '%s' when creating subscription", err)
	}

	subscriberCalled, otherCalled := false, false

	errs := sub.Subscribe(messagedb.Subscribers{
		messageType: func(m *messagedb.Message) {
			subscriberCalled = true

			sub.Unsubscribe()
		},
		"other": func(m *messagedb.Message) {
			otherCalled = true

			sub.Unsubscribe()
		},
	})

	for err := range errs {
		t.Errorf("unexpected error '%s' when subscribed", err)
	}

	if !subscriberCalled {
		t.Errorf("expected subscriber to have been called")
	}
	if otherCalled {
		t.Errorf("expected other to not have been called")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %s", err)
	}
}

func TestRead(t *testing.T) {
	var tests = []struct {
		name       string
		streamName string
		position   int
		batchSize  int
		messages   int
	}{
		{"stream", "stream-name", 0, 11, 10},
		{"category", "category", 1, 23, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("unexpected error '%s' when opening a stub database connection", err)
			}
			defer db.Close()

			columns := []string{"id", "name", "type", "position", "global_position", "data", "metadata", "time"}
			rows := mock.NewRows(columns)
			for i := 0; i < tt.messages; i++ {
				rows.AddRow(uuid.New(), tt.streamName, "type", i, i, nil, nil, time.Now())
			}

			mock.ExpectQuery(fmt.Sprintf("get_%s_messages", tt.name)).
				WithArgs(tt.streamName, tt.position, tt.batchSize).
				WillReturnRows(rows)

			m := messagedb.New(db)

			msgs, err := m.Read(tt.streamName, tt.position, tt.batchSize)
			if err != nil {
				t.Errorf("unexpected error '%s' when reading", err)
			}

			if len(msgs) != tt.messages {
				t.Errorf("expected %d messages, got %d", tt.messages, len(msgs))
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %s", err)
			}
		})
	}
}

func TestReadAll(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error '%s' when opening a stub database connection", err)
	}
	defer db.Close()

	streamName := "readall"

	columns := []string{"id", "name", "type", "position", "global_position", "data", "metadata", "time"}

	firstPage := mock.NewRows(columns)
	for i := 1; i <= 1000; i++ {
		firstPage.AddRow(uuid.New(), streamName, "type", i, i, nil, nil, time.Now())
	}
	secondPage := mock.NewRows(columns)
	for i := 1001; i <= 1337; i++ {
		secondPage.AddRow(uuid.New(), streamName, "type", i, i, nil, nil, time.Now())
	}

	mock.ExpectQuery("get_category_messages").
		WithArgs(streamName, 0, 1000).
		WillReturnRows(firstPage)
	mock.ExpectQuery("get_category_messages").
		WithArgs(streamName, 1000, 1000).
		WillReturnRows(secondPage)

	reader := messagedb.New(db)

	msgs, err := reader.ReadAll(streamName)
	if err != nil {
		t.Fatalf("unexpected error '%s' when reading all", err)
	}

	if len(msgs) != 1337 {
		t.Errorf("expected 1337 messages, got %d", len(msgs))
	}
}

func TestReadLast(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error '%s' when opening a stub database connection", err)
	}
	defer db.Close()

	streamName := "readlast"

	columns := []string{"id", "name", "type", "position", "global_position", "data", "metadata", "time"}

	mock.ExpectQuery("get_last_stream_message").
		WithArgs(streamName).
		WillReturnRows(mock.NewRows(columns).AddRow(uuid.New(), streamName, "type", 0, 0, nil, nil, time.Now()))

	m := messagedb.New(db)

	msg, err := m.ReadLast(streamName)
	if err != nil {
		t.Fatalf("unexpected error '%s' when reading last", err)
	}

	if msg.StreamName != streamName {
		t.Errorf("got %s, expected stream name %s", msg.StreamName, streamName)
	}
}

func TestWrite(t *testing.T) {
	var tests = []struct {
		name            string
		streamName      string
		messageType     string
		expectedVersion *int
		expect          func(sqlmock.Sqlmock, *messagedb.Message)
		handleError     func(error)
	}{
		{"stream name required", "", "type", nil, nil, func(err error) {
			if err != messagedb.ErrStreamNameRequired {
				t.Errorf("got %s, want error %s", err, messagedb.ErrStreamNameRequired)
			}
		}},
		{"type required", "stream", "", nil, nil, func(err error) {
			if err != messagedb.ErrTypeRequired {
				t.Errorf("got %s, want error %s", err, messagedb.ErrTypeRequired)
			}
		}},
		{"version conflict", "test", "type", nil, func(mock sqlmock.Sqlmock, msg *messagedb.Message) {
			mock.ExpectBegin()
			mock.ExpectQuery("write_message").
				WillReturnError(errors.New("Wrong Stream Version: 1337)"))
			mock.ExpectRollback()
		}, func(err error) {
			if _, ok := err.(messagedb.ErrVersionConflict); !ok {
				t.Errorf("got %s, want error version conflict", err)
			}
		}},
		{"valid", "stream", "type", nil,
			func(mock sqlmock.Sqlmock, msg *messagedb.Message) {
				null := []uint8("null")
				columns := []string{"next_position"}
				rows := mock.NewRows(columns).FromCSVString("0")
				mock.ExpectBegin()
				mock.ExpectQuery("write_message").
					WithArgs(msg.ID, msg.StreamName, msg.Type, null, null, msg.ExpectedVersion).
					WillReturnRows(rows)
				mock.ExpectCommit()
			},
			func(err error) {
				if err != nil {
					t.Errorf("got %s, want %v", err, nil)
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("unexpected error '%s' when opening a stub database connection", err)
			}
			defer db.Close()

			msg := messagedb.NewMessage(tt.streamName, tt.messageType)
			msg.ExpectedVersion = tt.expectedVersion

			if tt.expect != nil {
				tt.expect(mock, msg)
			}

			m := messagedb.New(db)

			if _, err = m.Write(msg); err != nil && tt.handleError != nil {
				tt.handleError(err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %s", err)
			}
		})
	}
}

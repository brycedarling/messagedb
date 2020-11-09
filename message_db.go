package messagedb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// MessageDB ...
type MessageDB interface {
	CreateSubscription(streamName, subscriberID string, subscribers Subscribers) Subscription
	Read(streamName string, position, batchSize int) (Messages, error)
	ReadAll(streamName string) (Messages, error)
	ReadLast(streamName string) (*Message, error)
	Write(*Message) (int, error)
}

// New ...
func New(db *sql.DB) MessageDB {
	return &messageDB{db}
}

type messageDB struct {
	db *sql.DB
}

var _ MessageDB = (*messageDB)(nil)

func (m *messageDB) CreateSubscription(streamName, subscriberID string, subscribers Subscribers) Subscription {
	return newSubscription(m, streamName, subscriberID, subscribers)
}

const (
	categoryMessagesSQL string = "SELECT * FROM get_category_messages($1, $2, $3)"
	streamMessagesSQL   string = "SELECT * FROM get_stream_messages($1, $2, $3)"
)

func (m *messageDB) Read(streamName string, position int, blockSize int) (msgs Messages, err error) {
	var query string
	if strings.Contains(streamName, "-") {
		// Entity streams have a dash
		query = streamMessagesSQL
	} else {
		// Category streams do not have a dash
		query = categoryMessagesSQL
	}

	rows, err := m.db.Query(query, streamName, position, blockSize)
	if err != nil {
		return msgs, err
	}
	defer rows.Close()

	for rows.Next() {
		msg, err := deserializeMessage(rows)
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

const blockSize int = 1000

func (m *messageDB) ReadAll(streamName string) (msgs Messages, err error) {
	position := 0
	var more Messages
	for {
		more, err = m.Read(streamName, position, blockSize)
		if err != nil {
			return msgs, err
		}

		msgs = append(msgs, more...)

		if len(more) != blockSize {
			return msgs, nil
		}

		position += blockSize
	}
}

const lastStreamMessageSQL string = "SELECT * FROM get_last_stream_message($1)"

func (m *messageDB) ReadLast(streamName string) (*Message, error) {
	return deserializeMessage(m.db.QueryRow(lastStreamMessageSQL, streamName))
}

type scanner interface {
	Scan(...interface{}) error
}

func deserializeMessage(row scanner) (*Message, error) {
	msg := &Message{}
	var (
		data     []byte
		metadata []byte
	)
	err := row.Scan(&msg.ID, &msg.StreamName, &msg.Type, &msg.Position, &msg.GlobalPosition, &data, &metadata, &msg.Time)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		err = json.Unmarshal(data, &msg.Data)
		if err != nil {
			return nil, err
		}
	}
	if len(metadata) > 0 {
		err = json.Unmarshal(metadata, &msg.Metadata)
		if err != nil {
			return nil, err
		}
	}
	return msg, nil
}

const writeSQL string = "SELECT message_store.write_message($1, $2, $3, $4, $5, $6)"

func (m *messageDB) Write(msg *Message) (int, error) {
	if len(msg.StreamName) == 0 {
		return 0, ErrStreamNameRequired
	}

	if len(msg.Type) == 0 {
		return 0, ErrTypeRequired
	}

	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	data, err := json.Marshal(msg.Data)
	if err != nil {
		return 0, err
	}

	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return 0, err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}

	res := tx.QueryRow(writeSQL, msg.ID, msg.StreamName, msg.Type, data, metadata, msg.ExpectedVersion)

	var nextPosition int
	err = res.Scan(&nextPosition)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return 0, err
		}
		return 0, handleWriteError(err, msg)
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return nextPosition, nil
}

var versionConflictRegex = regexp.MustCompile(".*Wrong.*Stream Version: (?P<ActualVersion>\\d+)\\)$")

func handleWriteError(err error, msg *Message) error {
	errorMatches := versionConflictRegex.FindStringSubmatch(err.Error())
	if len(errorMatches) == 0 {
		return err
	}
	var expectedVersion string
	if msg.ExpectedVersion != nil {
		expectedVersion = fmt.Sprintf("%d", *msg.ExpectedVersion)
	} else {
		expectedVersion = fmt.Sprintf("%v", nil)
	}
	actualVersion, err := strconv.Atoi(errorMatches[1])
	if err != nil {
		actualVersion = -1
	}
	log.Printf("Version conflict on %s stream. Expected version %s, actual %d", msg.StreamName, expectedVersion, actualVersion)
	return ErrVersionConflict
}

// ErrStreamNameRequired ...
var ErrStreamNameRequired = errors.New("missing stream name")

// ErrTypeRequired ...
var ErrTypeRequired = errors.New("missing type")

// ErrVersionConflict ...
var ErrVersionConflict = errors.New("version conflict")

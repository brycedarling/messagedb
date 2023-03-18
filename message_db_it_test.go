//go:build integration
// +build integration

package messagedb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/sethvargo/go-diceware/diceware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
)

const (
	defaultStoreUrl = "postgresql://message_store:@localhost:5432/message_store"
	defaultStoreDb  = "message_store"
	publicSchema    = "public"

	// EnvMessageStoreDb is the name of the environment variable that specifies the database used for message-db
	EnvMessageStoreDb = "MESSAGE_STORE_DB"

	// EnvDbUrl is the name of the environment variable that specifies the databae connection url used for message-db
	EnvDbUrl = "MESSAGE_STORE_DB_URL"
)

// Setup will be executed by NewDb against the database after establishing a connection.
type Setup func(*sql.DB) error

// NewDb establishes a connection to the database and executes any setup provided
func NewDb(driver string, conn string, setup Setup) (db *sql.DB, err error) {
	if db, err = sql.Open(driver, conn); err != nil {
		return nil, err
	}

	if setup != nil {
		return db, setup(db)
	}

	return db, nil
}

// Writes multiple messages to two different streams, and insures every message is read.
//
// This test uses unique stream names and subscriber ids for each test.
func Test_WriteMessage(t *testing.T) {
	s, err := NewDb("pgx", GetOrDefault(EnvDbUrl, defaultStoreUrl), func(db *sql.DB) error {
		_, err := db.Exec(fmt.Sprintf("SET search_path TO %s, %s", GetOrDefault(EnvMessageStoreDb, defaultStoreDb), publicSchema))
		return err
	})
	require.Nil(t, err, "error creating an sql.DB: %v", err)

	messageStore := New(s)

	tests := []struct {
		name         string
		file         string
		t            string
		stream       string
		subscriberId string
	}{
		{
			"message01",
			"testdata/message-user-01.json",
			"user",
			strings.Join(diceware.MustGenerate(2), "_"),
			strings.Join(diceware.MustGenerate(1), ""),
		},
		{
			"message02",
			"testdata/message-email-02.json",
			"email",
			strings.Join(diceware.MustGenerate(2), "_"),
			strings.Join(diceware.MustGenerate(1), ""),
		},
	}

	expectedMessageCount := 100

	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {

			f, err := os.Open(test.file)
			require.Nil(t, err, "error opening %s: %v", test.file, err)
			blob, err := ioutil.ReadAll(f)
			require.Nil(t, err, "unable to read content from %s: %v", test.file, err)
			data := make(map[string]interface{})
			require.Nil(t, json.Unmarshal(blob, &data), "unable to unmarshal blob from %s: %v", test.file, err)

			subscription, err := messageStore.CreateSubscription(test.stream, test.subscriberId)
			require.Nil(t, err, "error creating subscription for %s: %v", test.name, err)
			require.NotNil(t, subscription, "subscription was unexpectedly nil")

			wg := sync.WaitGroup{}
			wg.Add(expectedMessageCount)

			// keeps track of the messages we read
			// not only is this used by the test to insure we read what we wrote, it is a must-have as the subscriber
			// will see duplicate messages on each poll of the message db, because the write position is only advanced
			// every 100 messages.
			//
			// if this were a real application, we would need to be prepared to see duplicate messages anyway.
			readState := make(map[string]struct{})

			var subscribers Subscribers
			subscribers = map[string]Subscriber{
				test.t: func(m *Message) {

					// only update the read struct if we have never seen the message before
					if _, read := readState[m.ID]; !read {
						readState[m.ID] = struct{}{}
						// the JSON written to the message store and the json read from the message store are equivalent ...
						assert.Equal(t, data, m.Data, "message content differs from what was written")

						// ... but not identical
						bodyAsBlob, err := json.Marshal(m.Data)
						assert.Nil(t, err, "unexpected error marshaling message body: %v", err)
						assert.NotEqual(t, blob, bodyAsBlob, "message content is unexpectedly identical")

						wg.Done()
					} else {
						assert.Fail(t, fmt.Sprintf("Duplicate message received: stream '%s', message id '%s', stream position '%d', global position '%d'", m.StreamName, m.ID, m.Position, m.GlobalPosition))
					}
				},
			}

			// Start reading from the stream
			go func() {
				for e := range subscription.Subscribe(subscribers) {
					assert.Nil(t, e, "unexpected error reading from subscription: %v", e)
				}
			}()

			// Unsubscribe when we're done
			defer func() {
				subscription.Unsubscribe()
			}()

			// keeps track of the messages we write
			writtenState := make(map[string]struct{})

			// Write messages to the stream
			for i := 0; i < expectedMessageCount; i++ {
				m := &Message{
					ID:         uuid.New().String(),
					StreamName: test.stream,
					Type:       test.t,
					Data:       data,
					//Metadata:        nil,
					//ExpectedVersion: nil,
					//Position:        0,
					//GlobalPosition:  0,
					//Time:            time.Time{},
				}

				_, err := messageStore.Write(m)
				require.Nil(t, err, "error writing message %+v: %v", m, err)
				writtenState[m.ID] = struct{}{}
			}

			// Wait for subscriber to finish reading messages
			wg.Wait()

			assert.Equal(t, readState, writtenState)
			//useful if the above assertion (reflect.DeepEqual) fails
			for readId, _ := range readState {
				_, exists := writtenState[readId]
				assert.True(t, exists, "unexpected read of message id %s, it was never written?", readId)
			}

			for writtenId, _ := range writtenState {
				_, exists := readState[writtenId]
				assert.True(t, exists, "missing expected read of message id %s, it was never read?", writtenId)
			}

			assert.Equal(t, len(readState), len(writtenState), "expected messages read and written to be equal")
		})
	}
}

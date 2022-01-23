# Go Message DB Client

A barebones client for [message-db](https://github.com/message-db/message-db) implemented in Go.

Everything, including pub/sub, is exposed through the `MessageDB` interface.

```go
type MessageDB interface {
        CreateSubscription(streamName, subscriberID string, subscribers Subscribers) Subscription
        Read(streamName string, position, batchSize int) (Messages, error)
        ReadAll(streamName string) (Messages, error)
        ReadLast(streamName string) (*Message, error)
        Write(*Message) (int, error)
}
```


### Example Usage

Call `messagedb.New(db)` and provide it a `*sql.DB` and you are ready to go!

For example:

```go
package main

import (
	"database/sql"
	"log"

	"github.com/brycedarling/messagedb"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "dbname=message_store sslmode=disable user=postgres")
	if err != nil {
		log.Fatalf("unexpected error opening db: %s", err)
	}
	defer db.Close()

	db.Exec("SET search_path TO message_store,public;")

	m := messagedb.New(db)

	msgs, err := m.ReadAll("stream")
	if err != nil {
		log.Fatalf("unexpected error reading stream: %s", err)
	}

	log.Printf("Read %d messages from stream", len(msgs))
}

```

# Running integration tests

Integration tests require Docker to be installed and running before executing the integration tests.  They execute against version [MessageDB](https://github.com/message-db/message-db) `v1.2.6`, which is set in the `Dockerfile`. 

* `docker-compose build` creates the `local/messagedb` image
* `docker-compose up -d` starts a local Postgres instance and initializes the MessageDB
* `go test --tags integration` will run all tests, including the integration tests.

* `docker-compose down` will shut down MessageDB, but keep its state.  The next time it is started, the existing MessageDB will be used.
* `docker-compose down -v` will shut down MessageDB and destroy its state.  The next time it is started, a MessageDB will be initialized.

The default connection URL is `postgresql://message_store:@localhost:5432/message_store`, no password is required.

package messagedb

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// Subscription ...
type Subscription interface {
	Subscribe() error
	Unsubscribe()
}

// Subscriber ...
type Subscriber func(*Message)

// Subscribers ...
type Subscribers map[string]Subscriber

func newSubscription(messageDB MessageDB, streamName, subscriberID string, subscribers Subscribers) Subscription {
	return &subscription{
		messageDB:                      messageDB,
		streamName:                     streamName,
		subscriberID:                   subscriberID,
		subscriberStreamName:           fmt.Sprintf("subscriberPosition-%s", subscriberID),
		subscribers:                    subscribers,
		currentPosition:                0,
		messagesSinceLastPositionWrite: 0,
		isPolling:                      false,
		positionUpdateInterval:         99,
		messagesPerTick:                100,
		tickIntervalMS:                 100 * time.Millisecond,
	}
}

type subscription struct {
	messageDB                      MessageDB
	streamName                     string
	subscriberID                   string
	subscriberStreamName           string
	subscribers                    map[string]Subscriber
	currentPosition                int
	messagesSinceLastPositionWrite int
	isPolling                      bool
	positionUpdateInterval         int
	messagesPerTick                int
	tickIntervalMS                 time.Duration
}

var _ Subscription = (*subscription)(nil)

// ErrMissingSubscriberID ...
var ErrMissingSubscriberID = errors.New("missing subscriber id")

func (s *subscription) Subscribe() error {
	if s.streamName == "" {
		return ErrStreamNameRequired
	}
	if s.subscriberID == "" {
		return ErrMissingSubscriberID
	}

	log.Printf("Subscribing to %s as %s", s.streamName, s.subscriberID)

	if err := s.loadPosition(); err != nil {
		return err
	}

	s.poll()

	return nil
}

func (s *subscription) Unsubscribe() {
	log.Printf("Unsubscribing from %s as %s", s.streamName, s.subscriberID)

	s.isPolling = false
}

const readPositionKey string = "position"

func (s *subscription) loadPosition() error {
	msg, err := s.messageDB.ReadLast(s.subscriberStreamName)
	if err != nil {
		return err
	}
	if msg != nil {
		if position, ok := msg.Data[readPositionKey].(float64); ok {
			s.currentPosition = int(position)
		}
	}
	return nil
}

func (s *subscription) poll() {
	s.isPolling = true

	ticker := time.NewTicker(s.tickIntervalMS)
	quit := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				if !s.isPolling {
					close(quit)
				}
				s.tick()
			case <-quit:
				ticker.Stop()
			}
		}
	}()
}

func (s *subscription) tick() {
	msgs, err := s.nextBatchOfMessages()
	if err != nil {
		log.Printf("Error fetching batch: %s", err)
		s.Unsubscribe()
	}

	err = s.processBatch(msgs)
	if err != nil {
		log.Printf("Error processing batch: %s", err)
		s.Unsubscribe()
	}
}

func (s *subscription) nextBatchOfMessages() (Messages, error) {
	return s.messageDB.Read(s.streamName, s.currentPosition+1, s.messagesPerTick)
}

func (s *subscription) processBatch(msgs Messages) error {
	for _, msg := range msgs {
		subscriber, ok := s.subscribers[msg.Type]
		if !ok {
			continue
		}

		subscriber(msg)

		err := s.updateReadPosition(msg.GlobalPosition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *subscription) updateReadPosition(position int) error {
	s.currentPosition = position
	s.messagesSinceLastPositionWrite++

	if s.messagesSinceLastPositionWrite == s.positionUpdateInterval {
		// TODO: delete?
		if position < 1 {
			return ErrInvalidPosition
		}

		s.messagesSinceLastPositionWrite = 0

		msg := NewMessage(s.subscriberStreamName, "Read")
		msg.Data = map[string]interface{}{
			readPositionKey: position,
		}
		_, err := s.messageDB.Write(msg)
		return err
	}

	return nil
}

// ErrInvalidPosition ...
var ErrInvalidPosition = errors.New("invalid position")

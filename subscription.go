package messagedb

import (
	"errors"
	"fmt"
	"time"
)

// Subscription ...
type Subscription interface {
	Subscribe() chan error
	Unsubscribe()
}

// Subscriber ...
type Subscriber func(Subscription, *Message)

// Subscribers ...
type Subscribers map[string]Subscriber

func newSubscription(messageDB MessageDB, streamName, subscriberID string, subscribers Subscribers) (Subscription, error) {
	if streamName == "" {
		return nil, ErrStreamNameRequired
	}
	if subscriberID == "" {
		return nil, ErrSubscriberIDRequired
	}
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
	}, nil
}

// ErrSubscriberIDRequired ...
var ErrSubscriberIDRequired = errors.New("missing subscriber id")

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

func (s *subscription) Subscribe() chan error {
	errs := make(chan error)
	if err := s.loadPosition(); err != nil {
		errs <- err
		close(errs)
		return errs
	}
	s.poll(errs)
	return errs
}

func (s *subscription) Unsubscribe() {
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

func (s *subscription) poll(errs chan error) {
	s.isPolling = true

	ticker := time.NewTicker(s.tickIntervalMS)
	quit := make(chan struct{})

	go func() {
		for count := 0; ; count++ {
			select {
			case <-ticker.C:
				if err := s.tick(count); err != nil {
					errs <- err
					s.isPolling = false
				}
				if !s.isPolling {
					close(errs)
					close(quit)
				}
			case <-quit:
				ticker.Stop()
			}
		}
	}()
}

func (s *subscription) tick(count int) error {
	msgs, err := s.nextBatchOfMessages()
	if err != nil {
		return err
	}
	if err = s.processBatch(msgs); err != nil {
		return err
	}
	return nil
}

func (s *subscription) nextBatchOfMessages() (Messages, error) {
	return s.messageDB.Read(s.streamName, s.currentPosition+1, s.messagesPerTick)
}

func (s *subscription) processBatch(msgs Messages) error {
	for _, msg := range msgs {
		if subscriber, ok := s.subscribers[msg.Type]; ok {
			subscriber(s, msg)

			if err := s.updateReadPosition(msg.GlobalPosition); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *subscription) updateReadPosition(position int) error {
	s.currentPosition = position
	s.messagesSinceLastPositionWrite++

	if s.messagesSinceLastPositionWrite < s.positionUpdateInterval {
		return nil
	}

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

// ErrInvalidPosition ...
var ErrInvalidPosition = errors.New("invalid position")

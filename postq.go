package postq

import (
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// SyncEventHandlerFunc processes a single event and ONLY makes db changes.
type SyncEventHandlerFunc func(Context, Event) error

type SyncEventConsumer struct {
	WatchEvents     []string
	Consumers       []SyncEventHandlerFunc
	ConsumerOption  *ConsumerOption
	EventFetcherOpt *EventFetcherOption
}

func (t SyncEventConsumer) EventConsumer() (*PGConsumer, error) {
	return NewPGConsumer(t.Handle, t.ConsumerOption)
}

func (t *SyncEventConsumer) Handle(ctx Context) (int, error) {
	tx, err := ctx.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error initiating db tx: %w", err)
	}
	defer tx.Rollback(ctx)

	event, err := t.consumeOne(ctx, tx)
	if err != nil {
		if event != nil {
			now := time.Now()
			event.Attempts++
			event.Error = err.Error()
			event.LastAttempt = &now
			if err := event.Save(ctx, tx.Conn()); err != nil {
				return 0, fmt.Errorf("error saving updates of a failed event: %w", err)
			}
		}

		err = fmt.Errorf("error processing sync consumer: %w", err)
	}

	if event == nil {
		return 0, err
	}

	return 1, err
}

// consumeOne fetches a single event and passes it to all the consumers in one single transaction.
func (t *SyncEventConsumer) consumeOne(ctx Context, tx pgx.Tx) (*Event, error) {
	events, err := fetchEvents(ctx, tx, t.WatchEvents, 1, t.EventFetcherOpt)
	if err != nil {
		return nil, fmt.Errorf("error fetching events: %w", err)
	}

	if len(events) == 0 {
		return nil, nil
	}

	// sync consumers always fetch a single event at a time
	event := events[0]

	for _, syncConsumer := range t.Consumers {
		if err := syncConsumer(ctx, event); err != nil {
			return &event, err
		}
	}

	return &event, tx.Commit(ctx)
}

// AsyncEventHandlerFunc processes multiple events and returns the failed ones
type AsyncEventHandlerFunc func(Context, Events) Events

type AsyncEventConsumer struct {
	WatchEvents     []string
	BatchSize       int
	Consumer        AsyncEventHandlerFunc
	ConsumerOpt     *ConsumerOption
	EventFetcherOpt *EventFetcherOption
}

func (t *AsyncEventConsumer) Handle(ctx Context) (int, error) {
	tx, err := ctx.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error initiating db tx: %w", err)
	}
	defer tx.Rollback(ctx)

	events, err := fetchEvents(ctx, tx, t.WatchEvents, t.BatchSize, t.EventFetcherOpt)
	if err != nil {
		return 0, fmt.Errorf("error fetching events: %w", err)
	}

	failedEvents := t.Consumer(ctx, events)

	// TODO: use db NOW()
	now := time.Now()
	for i := range failedEvents {
		e := failedEvents[i]
		e.Attempts += 1
		e.LastAttempt = &now
	}

	if err := failedEvents.Update(ctx, tx.Conn()); err != nil {
		// TODO: More robust way to handle failed event insertion failures
		log.Printf("error inserting into table 'event_queue': %v\n", err)
	}

	return len(events), tx.Commit(ctx)
}

func (t AsyncEventConsumer) EventConsumer() (*PGConsumer, error) {
	return NewPGConsumer(t.Handle, t.ConsumerOpt)
}

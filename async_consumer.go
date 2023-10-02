package postq

import (
	"fmt"
	"log"
)

// AsyncEventHandlerFunc processes multiple events and returns the failed ones
type AsyncEventHandlerFunc func(Context, Events) Events

type AsyncEventConsumer struct {
	// Name of the events in the push queue to watch for.
	WatchEvents []string

	// Number of events to be fetched and processed at a time.
	BatchSize int

	// An async event handler that consumes events.
	Consumer AsyncEventHandlerFunc

	// ConsumerOption is the configuration for the PGConsumer.
	ConsumerOption *ConsumerOption

	// EventFetcherOption contains configuration on how the events should be fetched.
	EventFetcherOption *EventFetcherOption
}

func (t *AsyncEventConsumer) Handle(ctx Context) (int, error) {
	tx, err := ctx.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error initiating db tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	events, err := fetchEvents(ctx, tx, t.WatchEvents, t.BatchSize, t.EventFetcherOption)
	if err != nil {
		return 0, fmt.Errorf("error fetching events: %w", err)
	}

	failedEvents := t.Consumer(ctx, events)

	for i := range failedEvents {
		e := failedEvents[i]
		e.Attempts += 1
	}

	if err := failedEvents.Update(ctx, tx.Conn()); err != nil {
		// TODO: More robust way to handle failed event insertion failures
		log.Printf("error saving event attempt updates to event_queue: %v\n", err)
	}

	return len(events), tx.Commit(ctx)
}

func (t AsyncEventConsumer) EventConsumer() (*PGConsumer, error) {
	return NewPGConsumer(t.Handle, t.ConsumerOption)
}

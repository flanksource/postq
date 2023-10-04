package postq

import (
	"fmt"
	"log"
)

// SyncEventHandlerFunc processes a single event and ONLY makes db changes.
type SyncEventHandlerFunc func(Context, Event) error

type SyncEventConsumer struct {
	// Name of the events in the push queue to watch for.
	WatchEvents []string

	// List of sync event handlers that process a single event one after another in order.
	// All the handlers must succeed or else the event will be marked as failed.
	Consumers []SyncEventHandlerFunc

	// ConsumerOption is the configuration for the PGConsumer.
	ConsumerOption *ConsumerOption

	// EventFetcherOption contains configuration on how the events should be fetched.
	EventFetchOption *EventFetcherOption
}

func (t SyncEventConsumer) EventConsumer() (*PGConsumer, error) {
	return NewPGConsumer(t.Handle, t.ConsumerOption)
}

func (t *SyncEventConsumer) Handle(ctx Context) (int, error) {
	event, err := t.consumeEvent(ctx)
	if err != nil {
		if event == nil {
			return 0, err
		}

		event.Attempts++
		event.SetError(err.Error())
		const query = `UPDATE event_queue SET error=$1, attempts=$2, last_attempt=NOW() WHERE id=$3`
		if _, err := ctx.Pool().Exec(ctx, query, event.Error, event.Attempts, event.ID); err != nil {
			log.Printf("error saving event attempt updates to event_queue: %v\n", err)
		}
	}

	var eventCount int
	if event != nil {
		eventCount = 1
	}

	return eventCount, err
}

// consumeEvent fetches a single event and passes it to all the consumers in one single transaction.
func (t *SyncEventConsumer) consumeEvent(ctx Context) (*Event, error) {
	tx, err := ctx.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error initiating db tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	events, err := fetchEvents(ctx, tx, t.WatchEvents, 1, t.EventFetchOption)
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

// SyncHandlers converts the given user defined handlers into sync event handlers.
func SyncHandlers[T Context](fn ...func(ctx T, e Event) error) []SyncEventHandlerFunc {
	var syncHandlers []SyncEventHandlerFunc

	for _, f := range fn {
		syncHandler := func(ctx Context, e Event) error {
			return f(ctx.(T), e)
		}
		syncHandlers = append(syncHandlers, syncHandler)
	}

	return syncHandlers
}

package postq

import (
	"fmt"

	"github.com/jackc/pgx/v5"
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
	tx, err := ctx.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error initiating db tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	event, err := t.consumeEvent(ctx, tx)
	if err != nil {
		if event != nil {
			event.Attempts++
			event.SetError(err.Error())
			if err := event.Save(ctx, tx.Conn()); err != nil {
				return 0, fmt.Errorf("error saving updates of a failed event: %w", err)
			}
		}

		err = fmt.Errorf("error processing sync consumers: %w", err)
	}

	if event == nil {
		return 0, err
	}

	return 1, err
}

// consumeEvent fetches a single event and passes it to all the consumers in one single transaction.
func (t *SyncEventConsumer) consumeEvent(ctx Context, tx pgx.Tx) (*Event, error) {
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

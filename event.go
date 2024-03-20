package postq

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event represents the event queue table.
// The table must have the following fields.
type Event struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	Error       *string           `json:"error"`
	Attempts    int               `json:"attempts"`
	LastAttempt *time.Time        `json:"last_attempt"`
	Properties  map[string]string `json:"properties"`
	CreatedAt   time.Time         `json:"created_at"`
	Priority    int               `json:"priority"`
}

func (t Event) TableName() string {
	return "event_queue"
}

func (t *Event) SetError(err string) {
	t.Error = &err
}

// Scan scans pgx rows into Event
func (t *Event) Scan(rows pgx.Row) error {
	err := rows.Scan(
		&t.ID,
		&t.Name,
		&t.CreatedAt,
		&t.Properties,
		&t.Error,
		&t.LastAttempt,
		&t.Attempts,
		&t.Priority,
	)
	if err != nil {
		return err
	}

	return nil
}

type Events []Event

// Recreate creates the given failed events in batches after updating the
// attempts count.
func (events Events) Recreate(ctx Context, tx *pgx.Conn) error {
	if len(events) == 0 {
		return nil
	}

	var batch pgx.Batch
	for _, event := range events {
		attempts := event.Attempts + 1
		query := `INSERT INTO event_queue 
		(name, properties, error, last_attempt, attempts, priority) 
		VALUES($1, $2, $3, NOW(), $4, $5)`
		batch.Queue(query, event.Name, event.Properties, event.Error, attempts, event.Priority)
	}

	br := tx.SendBatch(ctx, &batch)
	defer br.Close()

	for {
		rows, err := br.Query()
		rows.Close()

		if err != nil {
			break
		}
	}

	return nil
}

type EventFetcherOption struct {
	// MaxAttempts is the number of times an event is attempted to process
	// default: 3
	MaxAttempts int

	// BaseDelay is the base delay between retries
	// default: 60 seconds
	BaseDelay int

	// Exponent is the exponent of the base delay
	// default: 5 (along with baseDelay = 60, the retries are 1, 6, 31, 156 (in minutes))
	Exponent int
}

// fetchEvents fetches given watch events from the `event_queue` table.
func fetchEvents(ctx Context, tx pgx.Tx, watchEvents []string, batchSize int, opts *EventFetcherOption) ([]Event, error) {
	if batchSize == 0 {
		batchSize = 1
	}
	const selectEventsQuery = `
		DELETE FROM event_queue
		WHERE id IN (
			SELECT id FROM event_queue
			WHERE
				attempts <= @maxAttempts AND
				name = ANY(@events) AND
				(last_attempt IS NULL OR last_attempt <= NOW() - INTERVAL '1 SECOND' * @baseDelay * POWER(attempts, @exponent))
			ORDER BY priority DESC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT @batchSize
		)
		RETURNING id, name, created_at, properties, error, last_attempt, attempts, priority
	`

	args := pgx.NamedArgs{
		"events":      watchEvents,
		"batchSize":   batchSize,
		"maxAttempts": 3,
		"baseDelay":   60,
		"exponent":    5,
	}

	if opts != nil {
		if opts.MaxAttempts > 0 {
			args["maxAttempts"] = opts.MaxAttempts
		}

		if opts.BaseDelay > 0 {
			args["baseDelay"] = opts.BaseDelay
		}

		if opts.Exponent > 0 {
			args["exponent"] = opts.Exponent
		}
	}

	rows, err := tx.Query(ctx, selectEventsQuery, args)
	if err != nil {
		return nil, fmt.Errorf("error selecting events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		if err := e.Scan(rows); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		events = append(events, e)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error iterating rows: %w", rows.Err())
	}

	if len(events) > 0 {
		ctx.Tracef("%s %d events fetched", strings.Join(watchEvents, ","), len(events))
	}
	return events, nil
}

package postq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event represents the event queue table.
// The table must have the following fields.
type Event struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	Properties  map[string]string `json:"properties"`
	Error       *string           `json:"error"`
	Attempts    int               `json:"attempts"`
	LastAttempt *time.Time        `json:"last_attempt"`
}

func (t *Event) SetError(err string) {
	t.Error = &err
}

// Scan scans pgx rows into Event
func (t *Event) Scan(rows pgx.Row) error {
	var propertiesJSON []byte
	err := rows.Scan(
		&t.ID,
		&t.Name,
		&propertiesJSON,
		&t.Error,
		&t.LastAttempt,
		&t.Attempts,
	)
	if err != nil {
		return err
	}

	if propertiesJSON != nil {
		err = json.Unmarshal(propertiesJSON, &t.Properties)
		if err != nil {
			return err
		}
	}

	return nil
}

// Save saves the event or updates it if it exists.
func (t *Event) Save(ctx Context, conn *pgx.Conn) error {
	propertiesJSON, err := json.Marshal(t.Properties)
	if err != nil {
		return err
	}

	var query string
	if t.ID == uuid.Nil {
		query = `INSERT INTO event_queue (name, properties, error, attempts, last_attempt) VALUES ($1, $2, $3, $4, NOW()) RETURNING id;`
		err = conn.QueryRow(ctx, query, t.Name, propertiesJSON, t.Error, t.Attempts).Scan(&t.ID)
	} else {
		query = `UPDATE event_queue SET name=$1, properties=$2, error=$3, attempts=$4, last_attempt=NOW() WHERE id=$5 RETURNING created_at;`
		_, err = conn.Exec(ctx, query, t.Name, propertiesJSON, t.Error, t.Attempts, t.ID)
	}

	return err
}

type Events []Event

// Update updates the events in batches.
func (events Events) Update(ctx Context, tx *pgx.Conn) error {
	if len(events) == 0 {
		return nil
	}

	var batch pgx.Batch
	for _, event := range events {
		propertiesJSON, err := json.Marshal(event.Properties)
		if err != nil {
			return err
		}

		query := `UPDATE event_queue SET name=$1, properties=$2, error=$3, attempts=$4, last_attempt=NOW() WHERE id=$5 RETURNING created_at;`
		batch.Queue(query, event.Name, propertiesJSON, event.Error, event.Attempts, event.ID)
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
		RETURNING id, name, properties, error, last_attempt, attempts
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

	return events, nil
}

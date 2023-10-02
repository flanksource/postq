package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/flanksource/postq"
	"github.com/sethvargo/go-retry"
)

// Defaults ...
var (
	DBReconnectMaxDuration         = time.Minute * 5
	DBReconnectBackoffBaseDuration = time.Second
)

// Listen listens to postgres notifications.
// On failure, it'll keep retrying with backoff
func Listen(ctx postq.Context, channel string, pgNotify chan<- string) {
	var listen = func(ctx postq.Context, pgNotify chan<- string) error {
		conn, err := ctx.Pool().Acquire(ctx)
		if err != nil {
			return fmt.Errorf("error acquiring database connection: %v", err)
		}
		defer conn.Release()

		_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
		if err != nil {
			return fmt.Errorf("error listening to database notifications: %v", err)
		}

		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				return fmt.Errorf("error listening to database notifications: %v", err)
			}

			pgNotify <- n.Payload
		}
	}

	// retry on failure.
	for {
		backoff := retry.WithMaxDuration(DBReconnectMaxDuration, retry.NewExponential(DBReconnectBackoffBaseDuration))
		err := retry.Do(ctx, backoff, func(retryContext context.Context) error {
			ctx := retryContext.(postq.Context)
			if err := listen(ctx, pgNotify); err != nil {
				return retry.RetryableError(err)
			}

			return nil
		})

		fmt.Printf("failed to connect to database: %v\n", err)
	}
}

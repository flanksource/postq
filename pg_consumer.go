package postq

import (
	"fmt"
	"time"
)

type ConsumerFunc func(ctx Context) (count int, err error)

// PGConsumer manages concurrent consumers to handle PostgreSQL NOTIFY events from a specific channel.
type PGConsumer struct {
	// Number of concurrent consumers
	numConsumers int

	// waitDurationOnFailure is the duration to wait before attempting to consume events again
	// after an unexpected failure.
	waitDurationOnFailure time.Duration

	// pgNotifyTimeout is the timeout to consume events in case no Consume notification is received.
	pgNotifyTimeout time.Duration

	// consumerFunc is responsible in fetching & consuming the events for the given batch size and events.
	// It returns the number of events it fetched.
	consumerFunc ConsumerFunc
}

type ConsumerOption struct {
	// Number of concurrent consumers.
	NumConsumers int

	// timeout is the timeout to call the consumer func in case no pg notification is received.
	timeout time.Duration

	// WaitDurationOnFailure is the duration to wait before attempting to call consumer func again
	// after an unexpected failure.
	WaitDurationOnFailure time.Duration
}

// NewPGConsumer returns a new EventConsumer
func NewPGConsumer(consumerFunc ConsumerFunc, opt *ConsumerOption) (*PGConsumer, error) {
	if consumerFunc == nil {
		return nil, fmt.Errorf("consumer func cannot be nil")
	}

	ec := &PGConsumer{
		numConsumers:          1,
		consumerFunc:          consumerFunc,
		pgNotifyTimeout:       time.Minute,
		waitDurationOnFailure: time.Second * 5,
	}

	if opt != nil {
		if opt.timeout != 0 {
			ec.pgNotifyTimeout = opt.timeout
		}

		if opt.NumConsumers > 0 {
			ec.numConsumers = opt.NumConsumers
		}

		if opt.WaitDurationOnFailure != 0 {
			ec.waitDurationOnFailure = opt.WaitDurationOnFailure
		}
	}

	return ec, nil
}

// ConsumeEventsUntilEmpty consumes events in a loop until the event queue is empty.
func (t *PGConsumer) ConsumeEventsUntilEmpty(ctx Context) {
	for {
		count, err := t.consumerFunc(ctx)
		if err != nil {
			time.Sleep(t.waitDurationOnFailure)
		} else if count == 0 {
			return
		}
	}
}

// Listen starts consumers in the background
func (e *PGConsumer) Listen(ctx Context, pgNotify <-chan string) {
	e.ConsumeEventsUntilEmpty(ctx)

	for i := 0; i < e.numConsumers; i++ {
		go func() {
			for {
				select {
				case <-pgNotify:
					e.ConsumeEventsUntilEmpty(ctx)

				case <-time.After(e.pgNotifyTimeout):
					e.ConsumeEventsUntilEmpty(ctx)
				}
			}
		}()
	}
}

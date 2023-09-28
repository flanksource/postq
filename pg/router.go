package pg

import (
	"strings"

	"github.com/flanksource/postq"
)

// notifyRouter distributes the pgNotify event to multiple channels
// based on the payload.
type notifyRouter struct {
	registry map[string]chan string

	// payloads to skip
	skip map[string]struct{}
}

func NewNotifyRouter() *notifyRouter {
	return &notifyRouter{
		registry: make(map[string]chan string),
		skip:     make(map[string]struct{}),
	}
}

func (t *notifyRouter) Skip(payloads ...string) *notifyRouter {
	for i := range payloads {
		t.skip[payloads[i]] = struct{}{}
	}

	return t
}

// RegisterRoutes creates a single channel for the given routes and returns it.
func (t *notifyRouter) RegisterRoutes(routes ...string) <-chan string {
	pgNotifyChannel := make(chan string)
	for _, we := range routes {
		t.registry[we] = pgNotifyChannel
	}

	return pgNotifyChannel
}

func (t *notifyRouter) Run(ctx postq.Context, channel string) {
	eventQueueNotifyChannel := make(chan string)
	go Listen(ctx, channel, eventQueueNotifyChannel)

	for payload := range eventQueueNotifyChannel {
		if _, ok := t.skip[payload]; ok || payload == "" {
			continue
		}

		// The original payload is expected to be in the form of
		// <route> <...optional payload>
		fields := strings.Fields(payload)
		route := fields[0]
		derivedPayload := strings.Join(fields[1:], " ")

		if ch, ok := t.registry[route]; ok {
			ch <- derivedPayload
		}
	}
}

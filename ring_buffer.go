package postq

import (
	"container/ring"
)

func getRecords(ringBuffer *ring.Ring) []Event {
	events := make([]Event, 0, ringBuffer.Len())
	ringBuffer.Do(func(v any) {
		if v == nil {
			return
		}

		e := v.(Event)
		events = append(events, e)
	})

	return events
}

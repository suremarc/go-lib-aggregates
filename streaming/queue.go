package main

import (
	"sync"
	"time"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

func isAggregateReady(aggregate globals.Aggregate) bool {
	return aggregate.EndTimestamp < ptime.IMillisecondsFromTime(time.Now())
}

type aggregateQueue struct {
	unpublished sync.Map
}

type index struct {
	ticker    string
	timestamp ptime.IMilliseconds
}

func (a *aggregateQueue) enqueue(aggregate globals.Aggregate) {
	a.unpublished.Store(index{
		ticker:    aggregate.Ticker,
		timestamp: aggregate.StartTimestamp,
	}, aggregate)
}

func (a *aggregateQueue) sweepAndClear(f func(globals.Aggregate) bool) {
	a.unpublished.Range(func(key, value any) bool {
		aggregate := value.(globals.Aggregate)
		if shouldDelete := f(aggregate); shouldDelete {
			a.unpublished.Delete(key)
		}

		return true
	})
}

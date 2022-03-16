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
	lock        sync.RWMutex
}

type index struct {
	ticker    string
	timestamp ptime.IMilliseconds
}

func (a *aggregateQueue) enqueue(aggregate globals.Aggregate) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	a.unpublished.Store(index{
		ticker:    aggregate.Ticker,
		timestamp: aggregate.StartTimestamp,
	}, aggregate)
}

func (a *aggregateQueue) sweepAndClear(f func(globals.Aggregate)) {
	a.lock.Lock()
	a.unpublished.Range(func(key, value any) bool {
		aggregate := value.(globals.Aggregate)
		if isAggregateReady(aggregate) {
			f(aggregate)
			a.unpublished.Delete(key)
		}
		return true
	})
	a.lock.Unlock()
}

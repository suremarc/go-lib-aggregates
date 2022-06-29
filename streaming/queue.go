package main

import (
	"sync"
	"time"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/suremarc/go-lib-aggregates/db"
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
	barLength db.BarLength
}

func (a *aggregateQueue) enqueue(aggregate globals.Aggregate, barLength db.BarLength) {
	a.unpublished.Store(index{
		ticker:    aggregate.Ticker,
		timestamp: aggregate.StartTimestamp,
		barLength: barLength,
	}, aggregate)
}

func (a *aggregateQueue) sweepAndClear(f func(globals.Aggregate, db.BarLength) bool) {
	a.unpublished.Range(func(key, value any) bool {
		aggregate := value.(globals.Aggregate)
		if shouldDelete := f(aggregate, key.(index).barLength); shouldDelete {
			a.unpublished.Delete(key)
		}

		return true
	})
}

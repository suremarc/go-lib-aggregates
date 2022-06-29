package db

import (
	"sync"
	"time"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type index struct {
	ticker    string
	timestamp ptime.IMilliseconds
	barLength BarLength
}

func snapTimestamp(ts ptime.INanoseconds) ptime.IMilliseconds {
	return ptime.IMillisecondsFromDuration(ts.ToDuration().Truncate(time.Minute))
}

type NativeDB struct {
	lockManager lockManager
	data        sync.Map
	lastUpdated sync.Map
	ttl         map[BarLength]time.Duration
	flushTicker *time.Ticker
}

var _ DB[Txn] = &NativeDB{}

func NewNativeDB() *NativeDB {
	n := &NativeDB{
		ttl: map[BarLength]time.Duration{
			BarLengthSecond: time.Minute * 15,
			BarLengthMinute: time.Minute * 15,
			BarLengthDay:    time.Hour * 24,
		},
		flushTicker: time.NewTicker(time.Minute * 15),
	}

	go func() {
		// TODO: add context cancellation
		for range n.flushTicker.C {
			n.Flush()
		}
	}()

	return n
}

func (n *NativeDB) Get(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength) globals.Aggregate {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: snapTimestamp(timestamp),
		barLength: barLength,
	}

	val, _ := n.data.LoadOrStore(index, globals.Aggregate{
		Ticker:         index.ticker,
		StartTimestamp: index.timestamp,
		EndTimestamp:   index.timestamp + ptime.IMillisecondsFromDuration(time.Minute),
	})
	return val.(globals.Aggregate)
}

func (n *NativeDB) Set(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength, aggregate globals.Aggregate) {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: snapTimestamp(timestamp),
		barLength: barLength,
	}

	n.data.Store(index, aggregate)
	n.lastUpdated.Store(index, ptime.INanosecondsFromTime(time.Now()))
}

func (n *NativeDB) Delete(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength) {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: snapTimestamp(timestamp),
		barLength: barLength,
	}

	n.data.Delete(index)
}

func (n *NativeDB) Flush() {
	n.data.Range((func(key, value any) bool {
		index := key.(index)
		var tx Txn
		n.maybeAcquireLock(&tx, index.ticker)
		defer n.Commit(&tx)

		lastUpdatedNanosAny, _ := n.lastUpdated.LoadOrStore(index, ptime.INanosecondsFromTime(time.Now()))
		lastUpdatedNanos := lastUpdatedNanosAny.(ptime.INanoseconds).ToDuration().Nanoseconds()

		ttl, ok := n.ttl[index.barLength]
		if ok && time.Since(time.Unix(lastUpdatedNanos/1_000_000_000, lastUpdatedNanos%1_000_000_000)) > ttl {
			n.Delete(&tx, index.ticker, index.timestamp.ToINanoseconds(), index.barLength)
		}

		return true
	}))
}

func (n *NativeDB) Commit(tx *Txn) {
	tx.lock.Unlock()
	*tx = Txn{}
}

func (n *NativeDB) Range(fn func(globals.Aggregate) bool) {
	n.data.Range(func(key, value any) bool {
		return fn(value.(globals.Aggregate))
	})
}

func (h *NativeDB) maybeAcquireLock(tx *Txn, ticker string) {
	if tx.Empty() {
		tx.ticker = ticker
		tx.lock = h.lockManager.acquire(ticker)
	} else if tx.ticker != ticker {
		panic("cannot acquire lock on multiple tickers")
	}
}

type Txn struct {
	ticker string
	lock   *sync.Mutex
}

func (t *Txn) Empty() bool {
	return t.lock == nil
}

type lockManager struct {
	locks sync.Map
}

func (l *lockManager) acquire(ticker string) *sync.Mutex {
	v, _ := l.locks.LoadOrStore(ticker, &sync.Mutex{})
	lock := v.(*sync.Mutex)
	lock.Lock()

	return lock
}

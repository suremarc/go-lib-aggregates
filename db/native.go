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
}

var _ DB[Txn] = &NativeDB{}

func NewNativeDB() *NativeDB {
	return &NativeDB{}
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

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
}

func truncateTimestamp(ts ptime.IMilliseconds) ptime.IMilliseconds {
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

func (n *NativeDB) Get(tx *Txn, ticker string, timestamp ptime.INanoseconds) globals.Aggregate {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: truncateTimestamp(timestamp.ToIMilliseconds()),
	}

	val, _ := n.data.LoadOrStore(index, globals.Aggregate{})
	return val.(globals.Aggregate)
}

func (n *NativeDB) Set(tx *Txn, ticker string, timestamp ptime.INanoseconds, aggregate globals.Aggregate) {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: truncateTimestamp(timestamp.ToIMilliseconds()),
	}

	n.data.Store(index, aggregate)
}

func (n *NativeDB) Delete(tx *Txn, ticker string, timestamp ptime.INanoseconds) {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: truncateTimestamp(timestamp.ToIMilliseconds()),
	}

	n.data.Delete(index)
}

func (n *NativeDB) Commit(tx *Txn) {
	tx.lock.Unlock()
	*tx = Txn{}
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
	return t.ticker == ""
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

package db

import (
	"context"
	"sync"
	"time"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/sirupsen/logrus"
)

type index struct {
	ticker    string
	timestamp ptime.IMilliseconds
	barLength BarLength
}

type NativeDB struct {
	lockManager lockManager
	data        sync.Map
	lastUpdated sync.Map
	ttl         map[BarLength]time.Duration
	flushTicker *time.Ticker
}

var _ DB[Tx] = &NativeDB{}

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

func (n *NativeDB) Get(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (globals.Aggregate, error) {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: snapTimestamp(timestamp),
		barLength: barLength,
	}

	duration, err := getBarLengthDuration(barLength)
	if err != nil {
		panic(err.Error())
	}

	val, _ := n.data.LoadOrStore(index, globals.Aggregate{
		Ticker:         index.ticker,
		StartTimestamp: index.timestamp,
		EndTimestamp:   index.timestamp + ptime.IMillisecondsFromDuration(duration),
	})
	return val.(globals.Aggregate), nil
}

func (n *NativeDB) Upsert(tx *Tx, aggregate globals.Aggregate) error {
	n.maybeAcquireLock(tx, aggregate.Ticker)

	barLength, err := getBarLength(aggregate)
	if err != nil {
		panic(err.Error())
	}

	index := index{
		ticker:    aggregate.Ticker,
		timestamp: aggregate.Timestamp,
		barLength: barLength,
	}

	n.data.Store(index, aggregate)
	n.lastUpdated.Store(index, ptime.INanosecondsFromTime(time.Now()))

	return nil
}

func (n *NativeDB) Delete(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error {
	n.maybeAcquireLock(tx, ticker)

	index := index{
		ticker:    ticker,
		timestamp: snapTimestamp(timestamp),
		barLength: barLength,
	}

	n.data.Delete(index)

	return nil
}

func (n *NativeDB) Flush() {
	n.data.Range((func(key, value any) bool {
		index := key.(index)
		var tx Tx
		n.maybeAcquireLock(&tx, index.ticker)
		defer n.Commit(&tx)

		lastUpdatedNanosAny, _ := n.lastUpdated.LoadOrStore(index, ptime.INanosecondsFromTime(time.Now()))
		lastUpdatedNanos := lastUpdatedNanosAny.(ptime.INanoseconds).ToDuration().Nanoseconds()

		ttl, ok := n.ttl[index.barLength]
		if ok && time.Since(time.Unix(lastUpdatedNanos/1_000_000_000, lastUpdatedNanos%1_000_000_000)) > ttl {
			if err := n.Delete(&tx, index.ticker, index.timestamp.ToINanoseconds(), index.barLength); err != nil {
				logrus.WithField("index", index).WithError(err).Error("couldn't delete row")
			}
		}

		return true
	}))
}

func (n *NativeDB) NewTx(context.Context) (*Tx, error) {
	return &Tx{}, nil
}

func (n *NativeDB) Commit(tx *Tx) error {
	tx.lock.Unlock()
	*tx = Tx{}

	return nil
}

func (n *NativeDB) Range(fn func(globals.Aggregate) bool) {
	n.data.Range(func(key, value any) bool {
		return fn(value.(globals.Aggregate))
	})
}

func (h *NativeDB) maybeAcquireLock(tx *Tx, ticker string) {
	if tx.Empty() {
		tx.ticker = ticker
		tx.lock = h.lockManager.acquire(ticker)
	} else if tx.ticker != ticker {
		panic("cannot acquire lock on multiple tickers")
	}
}

type Tx struct {
	ticker string
	lock   *sync.Mutex
}

func (t *Tx) Empty() bool {
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

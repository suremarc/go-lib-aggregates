package logic

import (
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/suremarc/go-lib-aggregates/db"
)

type Aggregable interface {
	GetTicker() string
	GetTimestamp() int64
}

type UpdateLogic[Trade any] func(globals.Aggregate, Trade) globals.Aggregate

func ProcessTrade[Txn any, Trade Aggregable](store db.DB[Txn], logic UpdateLogic[Trade], trade Trade, barLength db.BarLength) (globals.Aggregate, bool) {
	var tx Txn
	defer store.Commit(&tx)

	ts := parseTimestampFromInt64(trade.GetTimestamp())
	ticker := trade.GetTicker()

	aggregate := store.Get(&tx, ticker, ts, barLength)
	newAggregate := logic(aggregate, trade)
	updated := newAggregate != aggregate

	store.Set(&tx, ticker, ts, barLength, newAggregate)

	return newAggregate, updated
}

func parseTimestampFromInt64(x int64) ptime.INanoseconds {
	if x < 9999999999999 {
		return ptime.IMilliseconds(x).ToINanoseconds()
	}

	return ptime.INanoseconds(x)
}

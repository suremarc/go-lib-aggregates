package logic

import (
	"time"

	"github.com/polygon-io/go-lib-models/v2/currencies"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/suremarc/go-lib-aggregates/db"
)

func ProcessTrade[Txn any](store db.DB[Txn], trade currencies.Trade) (globals.Aggregate, bool) {
	var tx Txn
	defer store.Free(&tx)

	ts := trade.ExchangeTimestamp

	aggregate := store.Get(&tx, trade.Pair, ts)
	updated := UpdateAggregate(&aggregate, trade)

	store.Set(&tx, trade.Pair, ts, aggregate)

	return aggregate, updated
}

func UpdateAggregate(aggregate *globals.Aggregate, trade currencies.Trade) bool {
	var updated bool

	if aggregate.Open == 0 {
		updated = true
		aggregate.Open = trade.Price
	}

	if aggregate.Close == 0 {
		updated = true
		aggregate.Close = trade.Price
	}

	if trade.Price > aggregate.High {
		updated = true
		aggregate.High = trade.Price
	}

	if trade.Price < aggregate.Low || aggregate.Low == 0 {
		updated = true
		aggregate.Low = trade.Price
	}

	if aggregate.StartTimestamp == 0 {
		aggregate.StartTimestamp = truncateTimestamp(trade.ExchangeTimestamp.ToIMilliseconds())
		aggregate.EndTimestamp = truncateTimestamp(trade.ExchangeTimestamp.ToIMilliseconds() + ptime.IMillisecondsFromDuration(time.Minute))
	}

	aggregate.Ticker = trade.Pair

	return updated
}

func truncateTimestamp(ts ptime.IMilliseconds) ptime.IMilliseconds {
	return ptime.IMillisecondsFromDuration(ts.ToDuration().Truncate(time.Minute))
}

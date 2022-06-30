package logic

import (
	"context"
	"fmt"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/suremarc/go-lib-aggregates/db"
)

type Aggregable interface {
	GetTicker() string
	GetTimestamp() int64
}

type UpdateLogic[Trade any] func(globals.Aggregate, Trade) globals.Aggregate

func ProcessTrade[Txn any, Trade Aggregable](ctx context.Context, store db.DB[Txn], logic UpdateLogic[Trade], trade Trade, barLength db.BarLength) (agg globals.Aggregate, updated bool, err error) {
	tx, err := store.NewTx(ctx)
	if err != nil {
		return agg, false, fmt.Errorf("new tx: %w", err)
	}
	defer store.Commit(tx)

	ts := parseTimestampFromInt64(trade.GetTimestamp())
	ticker := trade.GetTicker()

	aggregate, err := store.Get(tx, ticker, ts, barLength)
	if err != nil {
		return agg, false, fmt.Errorf("get: %w", err)
	}

	newAggregate := logic(aggregate, trade)
	updated = newAggregate != aggregate

	if err := store.Insert(tx, newAggregate); err != nil {
		return agg, false, fmt.Errorf("set: %w", err)
	}

	return newAggregate, updated, nil
}

func parseTimestampFromInt64(x int64) ptime.INanoseconds {
	if x < 9999999999999 {
		return ptime.IMilliseconds(x).ToINanoseconds()
	}

	return ptime.INanoseconds(x)
}

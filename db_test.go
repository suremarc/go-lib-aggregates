package aggregates

import (
	"context"
	"math"
	"testing"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
)

func testLogic(agg globals.Aggregate, trade *stocks.Trade) globals.Aggregate {
	if agg.Open == 0 {
		agg.Open = trade.Price
	}

	agg.High = math.Max(agg.High, trade.Price)

	if agg.Low > trade.Price || agg.Low == 0 {
		agg.Low = trade.Price
	}

	agg.Close = trade.Price

	agg.Volume += float64(trade.Size_)

	return agg
}

func testDB[Tx any](t *testing.T, store db.DB[Tx]) {
	ctx := context.Background()

	trades := []stocks.Trade{
		{
			Base: stocks.Base{
				Ticker:    "PGON",
				Timestamp: 1,
			},
			Price: 1.0,
			Size_: 2,
		},
		{
			Base: stocks.Base{
				Ticker:    "PGON",
				Timestamp: 1,
			},
			Price: 2.0,
			Size_: 1,
		},
	}

	for _, trade := range trades {
		_, _, err := logic.ProcessTrade(ctx, store, testLogic, &trade, db.BarLengthMinute)
		require.NoError(t, err)
	}

	tx, err := store.NewTx(ctx)
	require.NoError(t, err)
	agg, err := store.Get(tx, "PGON", 0, db.BarLengthMinute)
	require.NoError(t, err)
	assert.Equal(t, 1.0, agg.Open)
	assert.Equal(t, 2.0, agg.High)
	assert.Equal(t, 1.0, agg.Low)
	assert.Equal(t, 2.0, agg.Close)
	assert.Equal(t, 3.0, agg.Volume)
}

func TestNativeDB(t *testing.T) {
	store := db.NewNativeDB(false)
	testDB[db.Tx](t, store)
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
	"gopkg.in/tomb.v2"
)

const barLength = db.BarLengthMinute

func main() {
	store := db.NewNativeDB()
	var publishQueue aggregateQueue

	t, ctx := tomb.WithContext(context.Background())

	client, err := polygonws.New(polygonws.Config{
		APIKey:  os.Getenv("API_KEY"),
		Feed:    polygonws.RealTime,
		Market:  polygonws.Stocks,
		RawData: true,
	})
	if err != nil {
		logrus.WithError(err).Fatal("initialize ws client")
	}

	trades := make(chan *stocks.Trade, 1000)
	t.Go(func() error { return consumerLoop(ctx, client, stocksTranslator, trades, polygonws.StocksTrades, "*") })

	for i := 0; i < 8; i++ {
		t.Go(func() error {
			return dbLoop(ctx, store, logic.StocksLogic, trades, &publishQueue)
		})
	}

	c := cron.New(cron.WithSeconds())
	c.AddFunc("* * * * * *", func() {
		publishQueue.sweepAndClear(func(aggregate globals.Aggregate, _ db.BarLength) bool {
			if !isAggregateReady(aggregate) {
				return false
			}

			fmt.Printf(
				"%s %s - open: $%.2f, close: $%.2f, high: $%.2f, low: $%.2f, volume: %f\n",
				aggregate.Ticker,
				aggregate.StartTimestamp.ToTime().Format("15:04:05"),
				aggregate.Open,
				aggregate.Close,
				aggregate.High,
				aggregate.Low,
				aggregate.Volume,
			)

			return true
		})
	})

	c.Start()

	if err := t.Wait(); err != nil {
		logrus.WithError(err).Fatal("died with error")
	}
}

type AggregableUnmarshaler interface {
	logic.Aggregable
	json.Unmarshaler
}

func consumerLoop[Trade any, PtrTrade interface {
	*Trade
	AggregableUnmarshaler
}](ctx context.Context, c *polygonws.Client, t jsonTranslator[PtrTrade], output chan<- PtrTrade, topic polygonws.Topic, subscriptions ...string) error {
	if err := c.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	if err := c.Subscribe(topic, subscriptions...); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output, ok := <-c.Output():
			if !ok {
				return nil
			}

			trade := PtrTrade(new(Trade))
			if err := trade.UnmarshalJSON([]byte(output.(json.RawMessage))); err != nil {
				return fmt.Errorf("unmarshal trade: %w", err)
			}
		}
	}
}

func dbLoop[Trade logic.Aggregable](ctx context.Context, store *db.NativeDB, updateLogic logic.UpdateLogic[Trade], input <-chan Trade, publishQueue *aggregateQueue) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
			defer cancel()
			// Unfortunately, Go will not infer that db.Txn is our type parameter, so we have to be explicit.
			aggregate, updated, err := logic.ProcessTrade[db.Tx](ctx, store, updateLogic, trade, barLength)
			if err != nil {
				logrus.WithError(err).Error("couldn't process trade")
				continue
			}

			if updated {
				publishQueue.enqueue(aggregate, barLength)
			}
		}
	}
}

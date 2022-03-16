package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/polygon-io/go-lib-models/v2/currencies"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
	"gopkg.in/tomb.v2"
)

func currenciesWorkerLoop(ctx context.Context, store *db.NativeDB, publishQueue, evictionQueue *aggregateQueue, input <-chan currencies.Trade) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			aggregate, updated := logic.ProcessTrade[db.Txn](store, logic.CurrenciesLogic, &trade)
			if updated {
				publishQueue.enqueue(aggregate)
			}

			evictionQueue.enqueue(aggregate)
		}
	}
}

func stocksWorkerLoop(ctx context.Context, store *db.NativeDB, publishQueue, evictionQueue *aggregateQueue, input <-chan stocks.Trade) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			aggregate, updated := logic.ProcessTrade[db.Txn](store, logic.StocksLogic, &trade)
			if updated {
				publishQueue.enqueue(aggregate)
			}

			evictionQueue.enqueue(aggregate)
		}
	}
}

func writeAndRead(c *websocket.Conn, msg string) error {
	_ = c.WriteMessage(websocket.TextMessage, []byte(msg))
	_, response, err := c.ReadMessage()
	if err != nil {
		return err
	}
	logrus.Info(string(response))

	return nil
}

func parseLoop[Trade any](ctx context.Context, url, subscribe string, output chan<- Trade) error {
	c, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	_, msg, err := c.ReadMessage()
	if err != nil {
		return err
	}
	logrus.Info(string(msg))

	writeAndRead(c, fmt.Sprintf(`{"action":"auth","params":"%s"}`, os.Getenv("API_KEY")))
	writeAndRead(c, fmt.Sprintf(`{"action":"subscribe","params":"%s"}`, subscribe))

	for {
		var trades []Trade
		if err := c.ReadJSON(&trades); err != nil {
			return err
		}

		// logrus.Info(trades)

		for _, trade := range trades {
			output <- trade
		}
	}
}

func displayLoop(ctx context.Context, input <-chan globals.Aggregate) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case aggregate := <-input:
			fmt.Println(aggregate)
		}
	}
}

func main() {
	store := db.NewNativeDB()
	var publishQueue, evictionQueue aggregateQueue

	t, ctx := tomb.WithContext(context.Background())

	trades := make(chan stocks.Trade, 1000)

	t.Go(func() error { return parseLoop(ctx, "wss://nasdaqfeed.polygon.io/stocks", "T.*", trades) })

	for i := 0; i < 1; i++ {
		t.Go(func() error {
			return stocksWorkerLoop(ctx, store, &publishQueue, &evictionQueue, trades)
		})
	}

	c := cron.New(cron.WithSeconds())
	c.AddFunc("* * * * * *", func() {
		publishQueue.sweepAndClear(func(aggregate globals.Aggregate) bool {
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
	c.AddFunc("0 * * * * *", func() {
		evictionQueue.sweepAndClear(func(aggregate globals.Aggregate) bool {
			shouldDelete := time.Since(aggregate.StartTimestamp.ToTime()) > 15*time.Minute
			if shouldDelete {
				var tx db.Txn
				store.Delete(&tx, aggregate.Ticker, aggregate.StartTimestamp.ToINanoseconds())
			}

			return shouldDelete
		})
	})
	c.Start()

	if err := t.Wait(); err != nil {
		logrus.WithError(err).Fatal("died with error")
	}
}

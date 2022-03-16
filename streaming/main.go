package main

import (
	"context"
	"fmt"
	"os"

	"github.com/gorilla/websocket"
	"github.com/polygon-io/go-lib-models/v2/currencies"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
	"gopkg.in/tomb.v2"
)

func workerLoop(ctx context.Context, store *db.NativeDB, queue *aggregateQueue, input <-chan currencies.Trade, output chan<- globals.Aggregate) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			aggregate, updated := logic.ProcessTrade[db.Txn](store, trade)
			if updated {
				queue.enqueue(aggregate)
			}
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

func parseLoop(ctx context.Context, output chan<- currencies.Trade) error {
	c, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://socket.polygon.io/crypto", nil)
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
	writeAndRead(c, fmt.Sprintf(`{"action":"subscribe","params":"%s"}`, "XT.*"))

	for {
		var trades []currencies.Trade
		if err := c.ReadJSON(&trades); err != nil {
			return err
		}

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
	var queue aggregateQueue

	t, ctx := tomb.WithContext(context.Background())

	trades := make(chan currencies.Trade, 1000)
	aggregates := make(chan globals.Aggregate, 1000)

	t.Go(func() error { return parseLoop(ctx, trades) })
	t.Go(func() error { return displayLoop(ctx, aggregates) })

	for i := 0; i < 8; i++ {
		t.Go(func() error { return workerLoop(ctx, store, &queue, trades, aggregates) })
	}

	c := cron.New()
	c.AddFunc("* * * * *", func() {
		queue.sweepAndClear(func(aggregate globals.Aggregate) {
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
		})
	})
	c.Start()

	if err := t.Wait(); err != nil {
		logrus.WithError(err).Fatal("died with error")
	}
}

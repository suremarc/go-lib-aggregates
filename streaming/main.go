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

const barLength = db.BarLengthMinute

func main() {
	store := db.NewNativeDB()
	var publishQueue aggregateQueue

	t, ctx := tomb.WithContext(context.Background())

	trades := make(chan websocketTrade, 1000)

	t.Go(func() error { return parseLoop(ctx, "wss://socket.polygon.io/stocks", "T.*", trades) })

	for i := 0; i < 8; i++ {
		t.Go(func() error {
			return stocksWorkerLoop(ctx, store, &publishQueue, trades)
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

type websocketTrade struct {
	EventType      string  `json:"ev"`
	Symbol         string  `json:"sym"`
	ID             string  `json:"i,omitempty"`
	Exchange       int     `json:"x,omitempty"`
	Price          float64 `json:"p,omitempty"`
	Size           int     `json:"s,omitempty"`
	Conditions     []int32 `json:"c,omitempty"`
	Timestamp      int     `json:"t,omitempty"`
	SequenceNumber int64   `json:"q,omitempty"`
	Tape           int     `json:"z,omitempty"`
}

func (w *websocketTrade) toStocksTrade() *stocks.Trade {
	return &stocks.Trade{
		Base: stocks.Base{
			Ticker:         w.Symbol,
			Timestamp:      int64(w.Timestamp * 1_000_000),
			SequenceNumber: w.SequenceNumber,
		},
		ID:         w.ID,
		Exchange:   int32(w.Exchange),
		Price:      w.Price,
		Size_:      uint32(w.Size),
		Conditions: w.Conditions,
		Tape:       int32(w.Tape),
	}
}

func currenciesWorkerLoop(ctx context.Context, store *db.NativeDB, publishQueue *aggregateQueue, input <-chan currencies.Trade) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
			defer cancel()
			// Unfortunately, Go will not infer that db.Txn is our type parameter, so we have to be explicit.
			aggregate, updated, err := logic.ProcessTrade[db.Tx](ctx, store, logic.CurrenciesLogic, &trade, barLength)
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

func stocksWorkerLoop(ctx context.Context, store *db.NativeDB, publishQueue *aggregateQueue, input <-chan websocketTrade) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case trade := <-input:
			ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
			defer cancel()
			// Same as above, we have to be explicit with our type parameter.
			aggregate, updated, err := logic.ProcessTrade[db.Tx](ctx, store, logic.StocksLogic, trade.toStocksTrade(), barLength)
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

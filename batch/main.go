package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/machinebox/progress"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/sirupsen/logrus"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"

	"github.com/klauspost/compress/zstd"
	"gopkg.in/tomb.v2"
)

func main() {
	store := db.NewNativeDB()

	t, ctx := tomb.WithContext(context.Background())

	trades := make(chan *stocks.Trade, 1000)
	t.Go(func() error { return tradesReaderLoop(ctx, trades) })

	t.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case trade, ok := <-trades:
				if !ok {
					return nil
				}

				if _, _, err := logic.ProcessTrade[db.Tx](ctx, store, logic.StocksLogic, trade, db.BarLengthMinute); err != nil {
					// fail open here
					logrus.WithError(err).Error("process trade")
				}
			}
		}
	})

	if err := t.Wait(); err != nil {
		logrus.WithError(err).Fatal("read trades")
	}

	out, err := os.Create("aggs.csv")
	if err != nil {
		logrus.WithError(err).Fatal("create output file")
	}
	defer out.Close()

	bw := bufio.NewWriter(out)
	defer bw.Flush()

	store.Range(func(a globals.Aggregate) bool {
		buf, err := a.MarshalCSV()
		if err != nil {
			logrus.WithError(err).Fatal("marshal csv")
			return false
		}

		if _, err := fmt.Fprintln(bw, string(buf)); err != nil {
			logrus.WithError(err).Fatal("push to writer")
			return false
		}

		return true
	})
}

type CSVUnmarshaler interface {
	FromCSV([]string) error
}

func tradesReaderLoop[Trade any, PtrTrade interface {
	*Trade
	CSVUnmarshaler
}](ctx context.Context, trades chan<- PtrTrade) error {
	defer close(trades)
	fi, err := os.Open("../trades-2022-06-24.csv.zst")
	if err != nil {
		return fmt.Errorf("open trades file: %w", err)
	}
	defer fi.Close()

	info, err := fi.Stat()
	if err != nil {
		return fmt.Errorf("stat trades file: %w", err)
	}

	pg := progress.NewReader(fi)
	go func() {
		for p := range progress.NewTicker(ctx, pg, info.Size(), 15*time.Second) {
			logrus.Infof(
				"%s/%s read (%.2f%% done, %v remaining)\n",
				humanize.IBytes(uint64(p.N())),
				humanize.IBytes(uint64(p.Size())),
				p.Percent(),
				p.Remaining().Round(time.Second))
		}
	}()

	zReader, err := zstd.NewReader(pg)
	if err != nil {
		return fmt.Errorf("open zstd reader: %w", err)
	}
	defer zReader.Close()

	r := csv.NewReader(zReader)
	r.Comma = '|'

	_, err = r.Read() // skip header
	if err != nil {
		return err
	}

	var record []string
	for record, err = r.Read(); err == nil; record, err = r.Read() {
		trade := PtrTrade(new(Trade))
		if err := trade.FromCSV(record); err != nil {
			return fmt.Errorf("unmarshal csv: %w", err)
		}

		trades <- trade
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

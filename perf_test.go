package aggregates

import (
	"context"
	"database/sql"
	"encoding/csv"
	"io"
	"os"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/machinebox/progress"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func BenchmarkSQLite(b *testing.B) {
	sqlDB, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(b, err)

	store, err := db.NewSQL(sqlDB)
	require.NoError(b, err)

	b.Log("Benchmarking SQLite")

	benchmarkDB[sql.Tx](b, store)
}

func BenchmarkNativeDB(b *testing.B) {
	n := db.NewNativeDB()

	benchmarkDB[db.Tx](b, n)
}

func benchmarkDB[Tx any](b *testing.B, store db.DB[Tx]) {
	tradesChan := make(chan stocks.Trade, 1000)
	go func() {
		defer close(tradesChan)
		fi, err := os.Open("./trades-2022-06-24.csv.zst")
		require.NoError(b, err)

		stat, err := fi.Stat()
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		progressReader := progress.NewReader(fi)
		go func() {
			for p := range progress.NewTicker(ctx, progressReader, stat.Size(), time.Second*15) {
				b.Logf(
					"%s/%s read (%.2f%% done, %v remaining)\n",
					humanize.IBytes(uint64(p.N())),
					humanize.IBytes(uint64(p.Size())),
					p.Percent(),
					p.Remaining().Round(time.Second))
			}
		}()

		zReader, err := zstd.NewReader(progressReader)
		require.NoError(b, err)
		defer zReader.Close()

		r := csv.NewReader(zReader)
		r.Comma = '|'

		_, err = r.Read() // skip header
		require.NoError(b, err)

		var record []string
		for record, err = r.Read(); err == nil; record, err = r.Read() {
			var trade stocks.Trade
			require.NoError(b, trade.FromCSV(record))
			tradesChan <- trade
		}

		require.ErrorIs(b, err, io.EOF)
	}()

	ctx := context.Background()

	var eg errgroup.Group
	for i := 0; i < 1; i++ {
		eg.Go(func() error {
			for trade := range tradesChan {
				if _, _, err := logic.ProcessTrade(ctx, store, logic.StocksLogic, &trade, db.BarLengthMinute); err != nil {
					return err
				}
			}

			return nil
		})
	}

	require.NoError(b, eg.Wait())
}

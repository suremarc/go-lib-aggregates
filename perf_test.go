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
	"github.com/go-redis/redis/v8"
	"github.com/machinebox/progress"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/suremarc/go-lib-aggregates/db"
	"github.com/suremarc/go-lib-aggregates/logic"
	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	_ "github.com/lib/pq"
)

func BenchmarkNativeDB(b *testing.B) {
	n := db.NewNativeDB()

	benchmarkDB[db.Tx](b, n, 4)
}

func BenchmarkRedis(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	store := db.NewRedis(client)
	benchmarkDB[db.RedisTx](b, store, 8)
}

func BenchmarkSQLiteInMemory(b *testing.B) {
	benchmarkSQL(b, db.DriverSQLite, "file::memory:?cache=shared&synchronous_commit=off", 1)
}

func BenchmarkSQLiteOnDisk(b *testing.B) {
	benchmarkSQL(b, db.DriverSQLite, "data.db; pragma journal_mode=WAL; pragma synchronous_commit=off; pragma mmap_size=30000000000; pragma temp_store=memory; pragma page_size=32768", 4)
}

func BenchmarkPostgresQL(b *testing.B) {
	benchmarkSQL(b, db.DriverPostgresQL, os.Getenv("POSTGRES_URL"), 4)
}

func benchmarkSQL(b *testing.B, driver db.Driver, dataSourceName string, concurrency int) {
	b.Log(driver)
	b.Log(dataSourceName)

	store, err := db.NewSQL(driver, dataSourceName)
	require.NoError(b, err)

	benchmarkDB[sql.Tx](b, store, concurrency)
}

func benchmarkDB[Tx any](b *testing.B, store db.DB[Tx], concurrency int) {
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
	for i := 0; i < concurrency; i++ {
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

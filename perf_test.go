package aggregates

import (
	"context"
	"database/sql"
	"encoding/csv"
	"io"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"
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
	benchmarkDB[db.RedisTx](b, store, 4)
}

func BenchmarkSQLiteInMemory(b *testing.B) {
	benchmarkSQL(b, db.DriverSQLite, "file::memory:?cache=shared", 1)
}

func BenchmarkSQLiteOnDisk(b *testing.B) {
	benchmarkSQL(b, db.DriverSQLite, "data.db", 1)
}

func BenchmarkPostgresQL(b *testing.B) {
	benchmarkSQL(b, db.DriverPostgresQL, "postgresql://localhost?sslmode=disable", 4)
}

func benchmarkSQL(b *testing.B, driver db.Driver, dataSourceName string, concurrency int) {
	sqlDB, err := sql.Open(string(driver), dataSourceName)
	require.NoError(b, err)

	_, err = sqlDB.Exec("DROP TABLE IF EXISTS aggregates")
	require.NoError(b, err)

	store, err := db.NewSQL(sqlDB)
	require.NoError(b, err)

	benchmarkDB[sql.Tx](b, store, concurrency)
}

func benchmarkDB[Tx any](b *testing.B, store db.DB[Tx], concurrency int) {
	tradesChan := make(chan stocks.Trade, 1000)
	go func() {
		defer close(tradesChan)
		fi, err := os.Open("./trades-2022-06-24.csv.zst")
		require.NoError(b, err)

		zReader, err := zstd.NewReader(fi)
		require.NoError(b, err)
		defer zReader.Close()

		r := csv.NewReader(zReader)
		r.Comma = '|'

		_, err = r.Read() // skip header
		require.NoError(b, err)

		var record []string
		var numRows int
		for record, err = r.Read(); err == nil; record, err = r.Read() {
			var trade stocks.Trade
			require.NoError(b, trade.FromCSV(record))
			tradesChan <- trade
			numRows++
			if numRows >= b.N {
				break
			}
		}

		if err != nil {
			require.ErrorIs(b, err, io.EOF)
		}
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

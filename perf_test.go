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

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	_ "github.com/lib/pq"
)

func getEnv(name, defaultVal string) string {
	result, ok := os.LookupEnv(name)
	if !ok {
		return defaultVal
	}

	return result
}

func BenchmarkNativeDB(b *testing.B) {
	n := db.NewNativeDB(false)

	benchmarkDB[db.Tx](b, n, true)
}

func BenchmarkRedis(b *testing.B) {
	benchmarkRedisProtocol(b, &redis.Options{
		Addr: getEnv("REDIS_URL", "localhost:6379"),
	})
}

func BenchmarkKeyDB(b *testing.B) {
	benchmarkRedisProtocol(b, &redis.Options{
		Addr: getEnv("KEYDB_URL", "localhost:6380"),
	})
}

func BenchmarkDragonfly(b *testing.B) {
	benchmarkRedisProtocol(b, &redis.Options{
		Addr: getEnv("DRAGONFLY_URL", "localhost:6381"),
	})
}

func benchmarkRedisProtocol(b *testing.B, opts *redis.Options) {
	client := redis.NewClient(opts)

	require.NoError(b, client.FlushAll(context.Background()).Err())

	store := db.NewRedis(client)
	benchmarkDB[db.RedisTx](b, store, true)
}

func BenchmarkSQLiteInMemory(b *testing.B) {
	benchmarkSQL(b, "sqlite", "file::memory:?cache=shared", false)
}

func BenchmarkSQLiteOnDisk(b *testing.B) {
	benchmarkSQL(b, "sqlite", "data.db", false)
}

func BenchmarkPostgresQL(b *testing.B) {
	benchmarkSQL(b, "postgres", getEnv("POSTGRES_URL", "postgresql://localhost?sslmode=disable&user=postgres&password=postgres"), true)
}

func benchmarkSQL(b *testing.B, driver, dataSourceName string, parallel bool) {
	sqlDB, err := sql.Open(string(driver), dataSourceName)
	require.NoError(b, err)

	_, err = sqlDB.Exec("DROP TABLE IF EXISTS aggregates")
	require.NoError(b, err)

	store, err := db.NewSQL(sqlDB)
	require.NoError(b, err)

	benchmarkDB[sql.Tx](b, store, parallel)
}

func benchmarkDB[Tx any](b *testing.B, store db.DB[Tx], parallel bool) {
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
	b.ResetTimer()

	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				trade := <-tradesChan
				if _, _, err := logic.ProcessTrade(ctx, store, logic.StocksLogic, &trade, db.BarLengthMinute); err != nil {
					b.Error(err)
				}
			}
		})
	} else {
		for trade := range tradesChan {
			if _, _, err := logic.ProcessTrade(ctx, store, logic.StocksLogic, &trade, db.BarLengthMinute); err != nil {
				b.Error(err)
			}
		}
	}
}

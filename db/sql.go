package db

import (
	"context"
	"database/sql"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type SQL struct {
	db *sql.DB
}

var _ DB[sql.Tx] = &SQL{}

func NewSQL(ctx context.Context, db *sql.DB) *SQL {
	db.ExecContext(ctx, sqlCreateTableStmt)
	return &SQL{
		db: db,
	}
}

const sqlCreateTableStmt = `CREATE TABLE IF NOT EXISTS aggregates (
	ticker VARCHAR(24),
	volume DOUBLE,
	vwap DOUBLE,
	open DOUBLE,
	close DOUBLE,
	high DOUBLE,
	low DOUBLE,
	timestamp BIGINT,
	transactions INT,
	bar_length CHAR(3)
)`

func (s *SQL) Get(tx *sql.Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (agg globals.Aggregate, err error) {
	ts := snapTimestamp(timestamp)

	if err := tx.QueryRow("SELECT (volume, vwap, open, close, high, low, transactions) FROM aggregates WHERE ticker = ? AND timestamp = ? AND bar_length = ?", ticker, ts, barLength).
		Scan(&agg.Volume, &agg.VWAP, &agg.Open, &agg.Close, &agg.High, &agg.Low, &agg.Transactions); err != nil {
		return agg, err
	}

	return agg, nil
}

func (s *SQL) Upsert(tx *sql.Tx, aggregate globals.Aggregate) error {
	barLength, err := getBarLength(aggregate)
	if err != nil {
		panic(err.Error())
	}

	_, err = tx.Exec(
		"INSERT INTO aggregates (ticker, volume, vwap, open, close, high, low, timestamp, transactions, bar_length) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		aggregate.Ticker,
		aggregate.Volume,
		aggregate.VWAP,
		aggregate.Open,
		aggregate.Close,
		aggregate.High,
		aggregate.Low,
		aggregate.Timestamp,
		aggregate.Transactions,
		barLength)
	if err != nil {
		return err
	}

	return nil
}

func (s *SQL) Delete(tx *sql.Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error {
	if _, err := tx.Exec("DELETE aggregates WHERE ticker = ? AND timestamp = ? AND bar_length = ?", ticker, timestamp, barLength); err != nil {
		return err
	}

	return nil
}

func (s *SQL) NewTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

func (s *SQL) Commit(tx *sql.Tx) error {
	return tx.Commit()
}

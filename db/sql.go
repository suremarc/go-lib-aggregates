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
	ticker VARCHAR(24) NOT NULL,
	volume DOUBLE NOT NULL,
	vwap DOUBLE NOT NULL,
	open DOUBLE NOT NULL,
	close DOUBLE NOT NULL,
	high DOUBLE NOT NULL,
	low DOUBLE NOT NULL,
	timestamp BIGINT NOT NULL,
	transactions INT NOT NULL,
	bar_length CHAR(3) NOT NULL,
	PRIMARY KEY (ticker, timestamp, bar_length);
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
		"INSERT INTO aggregates (ticker, volume, vwap, open, close, high, low, timestamp, transactions, bar_length) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE",
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

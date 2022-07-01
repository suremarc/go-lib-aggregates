package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type SQL struct {
	db *sql.DB
}

var _ DB[sql.Tx] = &SQL{}

func NewSQL(db *sql.DB) (*SQL, error) {
	_, err := db.Exec(sqlCreateTableStmt)
	if err != nil {
		return nil, err
	}

	return &SQL{
		db: db,
	}, nil
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
	PRIMARY KEY (ticker, timestamp, bar_length)
)`

func (s *SQL) Get(tx *sql.Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (agg globals.Aggregate, err error) {
	ts := snapTimestamp(timestamp)

	row := tx.QueryRow("SELECT volume, vwap, open, close, high, low, transactions FROM aggregates WHERE ticker = ? AND timestamp = ? AND bar_length = ?", ticker, ts, barLength)

	if err := row.Scan(&agg.Volume, &agg.VWAP, &agg.Open, &agg.Close, &agg.High, &agg.Low, &agg.Transactions); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			duration, err := getBarLengthDuration(barLength)
			if err != nil {
				tx.Rollback()
				return agg, err
			}

			return globals.Aggregate{
				Ticker:         ticker,
				StartTimestamp: ts,
				EndTimestamp:   ts + ptime.IMillisecondsFromDuration(duration),
			}, nil
		}

		return agg, err
	}

	return agg, nil
}

func (s *SQL) Upsert(tx *sql.Tx, aggregate globals.Aggregate) error {
	barLength, err := getBarLength(aggregate)
	if err != nil {
		tx.Rollback()
		return err
	}

	if err := s.Delete(tx, aggregate.Ticker, aggregate.Timestamp.ToINanoseconds(), barLength); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("delete: %w", err)
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
	if _, err := tx.Exec("DELETE FROM aggregates WHERE ticker = ? AND timestamp = ? AND bar_length = ?", ticker, timestamp, barLength); err != nil {
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

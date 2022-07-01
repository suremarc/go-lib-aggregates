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
	db         *sql.DB
	selectStmt *sql.Stmt
	insertStmt *sql.Stmt
	deleteStmt *sql.Stmt
}

var _ DB[sql.Tx] = &SQL{}

func NewSQL(db *sql.DB) (*SQL, error) {
	_, err := db.Exec(sqlCreateTableStmt)
	if err != nil {
		return nil, err
	}

	s := &SQL{
		db: db,
	}

	if s.selectStmt, err = db.Prepare(sqlSelectStmt); err != nil {
		return nil, fmt.Errorf("prepare select: %w", err)
	}

	if s.insertStmt, err = db.Prepare(sqlInsertStmt); err != nil {
		return nil, fmt.Errorf("prepare insert: %w", err)
	}

	if s.deleteStmt, err = db.Prepare(sqlDeleteStmt); err != nil {
		return nil, fmt.Errorf("prepare delete: %w", err)
	}

	return s, nil
}

const (
	sqlCreateTableStmt = `CREATE TABLE IF NOT EXISTS aggregates (
	ticker VARCHAR(24) NOT NULL,
	volume DOUBLE PRECISION NOT NULL,
	vwap DOUBLE PRECISION NOT NULL,
	open DOUBLE PRECISION NOT NULL,
	close DOUBLE PRECISION NOT NULL,
	high DOUBLE PRECISION NOT NULL,
	low DOUBLE PRECISION NOT NULL,
	timestamp BIGINT NOT NULL,
	transactions INT NOT NULL,
	bar_length CHAR(3) NOT NULL,
	PRIMARY KEY (ticker, timestamp, bar_length)
)`

	sqlSelectStmt = `SELECT volume, vwap, open, close, high, low, transactions FROM aggregates WHERE ticker=$1 AND timestamp=$2 AND bar_length=$3`
	sqlInsertStmt = `INSERT INTO aggregates (ticker, volume, vwap, open, close, high, low, timestamp, transactions, bar_length) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	sqlDeleteStmt = `DELETE FROM aggregates WHERE ticker=$1 AND timestamp=$2 AND bar_length=$3`
)

func (s *SQL) Get(tx *sql.Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (agg globals.Aggregate, err error) {
	ts := snapTimestamp(timestamp)

	row := tx.Stmt(s.selectStmt).QueryRow(ticker, ts, barLength)

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

	_, err = tx.Stmt(s.insertStmt).Exec(
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
	if _, err := tx.Stmt(s.deleteStmt).Exec(ticker, timestamp, barLength); err != nil {
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

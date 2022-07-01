package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

type Driver string

const (
	DriverPostgresQL = "postgres"
	DriverSQLite     = "sqlite"
)

func NewSQL(driver Driver, dataSource string) (*SQL, error) {
	db, err := sql.Open(string(driver), dataSource)

	_, err = db.Exec(getSQLStmt(driver, sqlCreateTableStmt))
	if err != nil {
		return nil, err
	}

	s := &SQL{
		db: db,
	}

	if s.selectStmt, err = db.Prepare(getSQLStmt(driver, sqlSelectStmt)); err != nil {
		return nil, fmt.Errorf("prepare select: %w", err)
	}

	if s.insertStmt, err = db.Prepare(getSQLStmt(driver, sqlInsertStmt)); err != nil {
		return nil, fmt.Errorf("prepare insert: %w", err)
	}

	if s.deleteStmt, err = db.Prepare(getSQLStmt(driver, sqlDeleteStmt)); err != nil {
		return nil, fmt.Errorf("prepare delete: %w", err)
	}

	return s, nil
}

func getSQLStmt(d Driver, stmt string) string {
	if d == DriverPostgresQL {
		stmt = strings.ReplaceAll(stmt, "DOUBLE", "DOUBLE PRECISION")

		i := 1
		for strings.Contains(stmt, "?") {
			stmt = strings.Replace(stmt, "?", fmt.Sprintf("$%d", i), 1)
			i += 1
		}
	}

	return stmt
}

const (
	sqlCreateTableStmt = `CREATE TABLE IF NOT EXISTS aggregates (
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

	sqlSelectStmt = `SELECT volume, vwap, open, close, high, low, transactions FROM aggregates WHERE ticker=? AND timestamp=? AND bar_length=?`
	sqlInsertStmt = `INSERT INTO aggregates (ticker, volume, vwap, open, close, high, low, timestamp, transactions, bar_length) VALUES (?,?,?,?,?,?,?,?,?,?)`
	sqlDeleteStmt = `DELETE FROM aggregates WHERE ticker=? AND timestamp=? AND bar_length=?`
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

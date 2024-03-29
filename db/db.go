package db

import (
	"context"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type BarLength string

const (
	BarLengthSecond BarLength = "sec"
	BarLengthMinute BarLength = "min"
	BarLengthDay    BarLength = "day"
)

// DB stores aggregates and exposes composable primitives that can be called concurrently.
// Tx denotes a transaction. Any two transactions must be completely read/write isolated
// from one another until DB.Commit is called on the transaction.
// Any implementation of this interface MUST roll back the transaction *automatically* if any
// error occurs during one of the operations.
type DB[Tx any] interface {
	// NewTx creates a fresh transaction with no operations associated with it.
	// Semantically, every operation in the transaction is guaranteed exclusive access to all rows it touches.
	// If any of these operations fail, the transaction must automatically be rolled back.
	// If no error occurs, Commit MUST be called, otherwise the implementation is not guaranteed to avoid leaks.
	NewTx(context.Context) (*Tx, error)

	// Get retrieves the aggregate with the given ticker and bar length that contains the requested timestamp.
	Get(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (globals.Aggregate, error)

	// Upsert upserts an aggregate.
	Upsert(tx *Tx, aggregate globals.Aggregate) error

	// Delete deletes an aggregate.
	Delete(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error

	// Commit commits the transaction to the database.
	// If the transaction cannot be committed for some reason, it is automatically rolled back,
	// and Commit returns the error.
	Commit(tx *Tx) error
}

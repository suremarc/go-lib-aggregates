package db

import (
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type BarLength string

const (
	BarLengthSecond BarLength = "sec"
	BarLengthMinute BarLength = "min"
	BarLengthDay    BarLength = "day"
)

// TODO: make methods fallible (return an error)
// Leaving this out for now for the sake of simplicity of demonstration.
// Obviously production-ready code should account for the case of errors.

type DB[Txn any] interface {
	Get(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength) globals.Aggregate
	Set(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength, aggregate globals.Aggregate)
	Delete(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength)

	// Range cannot be composed together with other operations.
	// Hence it does not take a *Txn.
	// Note: since this is a very special operation, it might be
	// worth extracting into a separate sub-interface,
	// or leaving it out of this interface altogether.
	Range(func(globals.Aggregate) bool)

	Commit(tx *Txn)
}

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

	Commit(tx *Txn)
}

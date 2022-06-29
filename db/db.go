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

type DB[Txn any] interface {
	Get(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength) globals.Aggregate
	Set(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength, aggregate globals.Aggregate)
	Delete(tx *Txn, ticker string, timestamp ptime.INanoseconds, barLength BarLength)

	// List cannot be composed together with other operations.
	// Hence it does not take a *Txn.
	List(filter func(globals.Aggregate) bool) []globals.Aggregate

	Commit(tx *Txn)
}

package db

import (
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

type DB[Txn any] interface {
	Get(tx *Txn, ticker string, timestamp ptime.INanoseconds) globals.Aggregate
	Set(tx *Txn, ticker string, timestamp ptime.INanoseconds, aggregate globals.Aggregate)

	Commit(tx *Txn)
}

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

type DB[Tx any] interface {
	Get(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (globals.Aggregate, error)
	Insert(tx *Tx, aggregate globals.Aggregate) error
	Delete(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error

	NewTx(context.Context) (*Tx, error)
	Commit(tx *Tx) error
}

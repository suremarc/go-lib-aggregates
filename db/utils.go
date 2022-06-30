package db

import (
	"errors"
	"time"

	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
)

var ErrInvalidBarLength = errors.New("unrecognized bar length")

func getBarLength(agg globals.Aggregate) (BarLength, error) {
	switch agg.EndTimestamp - agg.StartTimestamp {
	case ptime.IMillisecondsFromDuration(time.Second):
		return BarLengthSecond, nil
	case ptime.IMillisecondsFromDuration(time.Minute):
		return BarLengthMinute, nil
	case ptime.IMillisecondsFromDuration(time.Hour * 24):
		return BarLengthDay, nil
	default:
		return "", ErrInvalidBarLength
	}
}

func getBarLengthDuration(b BarLength) (time.Duration, error) {
	switch b {
	case BarLengthSecond:
		return time.Second, nil
	case BarLengthMinute:
		return time.Minute, nil
	case BarLengthDay:
		return time.Hour * 24, nil
	default:
		return 0, ErrInvalidBarLength
	}
}

func snapTimestamp(ts ptime.INanoseconds) ptime.IMilliseconds {
	return ptime.IMillisecondsFromDuration(ts.ToDuration().Truncate(time.Minute))
}

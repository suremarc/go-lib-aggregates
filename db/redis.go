package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/ptime"
	"github.com/sirupsen/logrus"
)

type Redis struct {
	client *redis.Client
	ttl    map[BarLength]time.Duration
}

type RedisTx struct {
	ctx      context.Context
	pipeline redis.Pipeliner
}

var _ DB[RedisTx] = &Redis{}

func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		client: client,
		ttl: map[BarLength]time.Duration{
			BarLengthSecond: time.Minute * 15,
			BarLengthMinute: time.Minute * 15,
			BarLengthDay:    time.Hour * 24,
		},
	}
}

func (r *Redis) Get(tx *RedisTx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (globals.Aggregate, error) {
	key := fmt.Sprintf("%s/%d/%s", ticker, snapTimestamp(timestamp), barLength)

	result := tx.pipeline.Get(tx.ctx, key)
	if result.Val() == "" {
		duration, err := getBarLengthDuration(barLength)
		if err != nil {
			tx.pipeline.Discard()
			return globals.Aggregate{}, err
		}

		ts := snapTimestamp(timestamp)
		defaultAgg := globals.Aggregate{
			Ticker:         ticker,
			StartTimestamp: ts,
			EndTimestamp:   ts + ptime.IMillisecondsFromDuration(duration),
		}

		if err := r.Upsert(tx, defaultAgg); err != nil {
			return globals.Aggregate{}, err
		}

		return defaultAgg, nil
	}

	if result.Err() != nil {
		tx.pipeline.Discard()
		return globals.Aggregate{}, result.Err()
	}

	logrus.Info(result.Val())
	var agg globals.Aggregate
	if err := json.Unmarshal([]byte(result.Val()), &agg); err != nil {
		tx.pipeline.Discard()
		return agg, fmt.Errorf("unmarshal: %w", err)
	}

	return agg, nil
}

func (r *Redis) Upsert(tx *RedisTx, aggregate globals.Aggregate) error {
	barLength, err := getBarLength(aggregate)
	if err != nil {
		tx.pipeline.Discard()
		return err
	}

	aggregateJSON, err := aggregate.MarshalJSON()
	if err != nil {
		tx.pipeline.Discard()
		return err
	}

	key := fmt.Sprintf("%s/%d/%s", aggregate.Ticker, aggregate.Timestamp, barLength)
	tx.pipeline.Set(tx.ctx, string(key), aggregateJSON, r.ttl[barLength])

	return nil
}

func (r *Redis) Delete(tx *RedisTx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error {
	key := fmt.Sprintf("%s/%d/%s", ticker, timestamp, barLength)
	tx.pipeline.Del(tx.ctx, key)

	return nil
}

func (r *Redis) NewTx(ctx context.Context) (*RedisTx, error) {
	return &RedisTx{
		ctx:      ctx,
		pipeline: r.client.TxPipeline(),
	}, nil
}

func (r *Redis) Commit(tx *RedisTx) error {
	_, err := tx.pipeline.Exec(tx.ctx)
	return err
}

# go-lib-aggregates

This project houses an implementation of OHLCV financial time-series aggregates. The main goals are the following:
1). Make the code more understandable through separation of concerns, so that each piece is individually understandable
2). Make the code reusable in different contexts, particularly for both stream and batch processing
In order to achieve this, several components of the logic were separated out into abstract components. As a result, this project contains four separate submodules: `db`, `logic`, `streaming`, and `batch`. Roughly, `db` and `logic` are the essential components of aggregation, whereas `streaming` and `batch` employ those essential components to their own differing ends. An in-depth view of the application architecture follows.

## `db`

`db` contains the cornerstone of the entire project, the `DB` interface. `DB` is the main container of state for an application that computes aggregates. It contains these methods:

```
type DB[Tx any] interface {
	NewTx(context.Context) (*Tx, error)
	Get(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) (globals.Aggregate, error)
	Upsert(tx *Tx, aggregate globals.Aggregate) error
	Delete(tx *Tx, ticker string, timestamp ptime.INanoseconds, barLength BarLength) error
	Commit(tx *Tx) error
}
```

It is generic over `Tx` to allow for different implementations with different transaction types, e.g. a SQL implementation would use `sql.Tx`. The importance of transactions is that they allow us to make the API more composable, due to the composability of transactions.

## `logic`

`logic` houses functions that update the database given an incoming trade, as well as smaller-scoped functions that update aggregates individually. Possible more advanced use-cases include stateful computations that need to store additional values inside the database (which would require modifying the DB interface), or having separate logic for daily and intraday aggregates.

## More to come...

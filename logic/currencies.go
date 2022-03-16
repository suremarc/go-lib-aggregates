package logic

import (
	"github.com/polygon-io/go-lib-models/v2/currencies"
	"github.com/polygon-io/go-lib-models/v2/globals"
)

func CurrenciesLogic(aggregate globals.Aggregate, trade *currencies.Trade) globals.Aggregate {
	if aggregate.Open == 0 {
		aggregate.Open = trade.Price
	}

	if aggregate.Close == 0 {
		aggregate.Close = trade.Price
	}

	if trade.Price > aggregate.High {
		aggregate.High = trade.Price
	}

	if trade.Price < aggregate.Low || aggregate.Low == 0 {
		aggregate.Low = trade.Price
	}

	aggregate.Volume += trade.OrderSize
	aggregate.Transactions++

	return aggregate
}

package logic

import (
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
)

var _ UpdateLogic[*stocks.Trade] = StocksLogic

// TODO: showcase separate intraday and EOD logic
func StocksLogic(aggregate globals.Aggregate, trade *stocks.Trade) globals.Aggregate {
	if stocksCanUpdateHighLow(trade) {
		if aggregate.Open == 0 {
			aggregate.Open = trade.Price
		}

		if aggregate.Close == 0 {
			aggregate.Close = trade.Price
		}
	}

	if stocksCanUpdateOpenClose(trade) {
		if trade.Price > aggregate.High {
			aggregate.High = trade.Price
		}

		if trade.Price < aggregate.Low || aggregate.Low == 0 {
			aggregate.Low = trade.Price
		}
	}

	if stocksCanUpdateVolume(trade) {
		aggregate.VWAP *= aggregate.Volume
		aggregate.Volume += float64(trade.Size_)
		aggregate.VWAP += trade.Price
		aggregate.VWAP /= aggregate.Volume

		aggregate.Transactions++
	}

	return aggregate
}

func stocksCanUpdateHighLow(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 2, 7, 15, 16, 20, 21, 22, 29, 37, 52:
			return false
		}
	}

	return true
}

func stocksCanUpdateOpenClose(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 2, 5, 7, 10, 12, 13, 15, 16, 17, 20, 21, 22, 29, 32, 33, 37, 38, 52:
			return false
		}
	}

	return true
}

func stocksCanUpdateVolume(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 15, 16:
			return false
		}
	}

	return true
}

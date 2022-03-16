package logic

import (
	"github.com/polygon-io/go-lib-models/v2/globals"
	"github.com/polygon-io/go-lib-models/v2/stocks"
)

var _ UpdateLogic[*stocks.Trade] = StocksLogic

func StocksLogic(aggregate globals.Aggregate, trade *stocks.Trade) globals.Aggregate {
	if canUpdateHighLow(trade) {
		if aggregate.Open == 0 {
			aggregate.Open = trade.Price
		}

		if aggregate.Close == 0 {
			aggregate.Close = trade.Price
		}
	}

	if canUpdateOpenClose(trade) {
		if trade.Price > aggregate.High {
			aggregate.High = trade.Price
		}

		if trade.Price < aggregate.Low || aggregate.Low == 0 {
			aggregate.Low = trade.Price
		}
	}

	if canUpdateVolume(trade) {
		aggregate.VWAP *= aggregate.Volume
		aggregate.Volume += float64(trade.Size_)
		aggregate.VWAP += trade.Price
		aggregate.VWAP /= aggregate.Volume

		aggregate.Transactions++
	}

	return aggregate
}

func canUpdateHighLow(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 2, 7, 15, 16, 20, 21, 22, 29, 37, 52:
			return false
		}
	}

	return true
}

func canUpdateOpenClose(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 2, 5, 7, 10, 12, 13, 15, 16, 17, 20, 21, 22, 29, 32, 33, 37, 38, 52:
			return false
		}
	}

	return true
}

func canUpdateVolume(trade *stocks.Trade) bool {
	for _, c := range trade.Conditions {
		switch c {
		case 15, 16:
			return false
		}
	}

	return true
}

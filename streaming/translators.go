package main

import (
	"encoding/json"

	"github.com/polygon-io/go-lib-models/v2/currencies"
	"github.com/polygon-io/go-lib-models/v2/stocks"
	"github.com/suremarc/go-lib-aggregates/logic"
)

type jsonTranslator[Output logic.Aggregable] func([]byte) (Output, error)

type websocketTrade struct {
	EventType      string  `json:"ev"`
	Symbol         string  `json:"sym"`
	ID             string  `json:"i,omitempty"`
	Exchange       int     `json:"x,omitempty"`
	Price          float64 `json:"p,omitempty"`
	Size           int     `json:"s,omitempty"`
	Conditions     []int32 `json:"c,omitempty"`
	Timestamp      int     `json:"t,omitempty"`
	SequenceNumber int64   `json:"q,omitempty"`
	Tape           int     `json:"z,omitempty"`
}

func (w *websocketTrade) toStocksTrade() *stocks.Trade {
	return &stocks.Trade{
		Base: stocks.Base{
			Ticker:         w.Symbol,
			Timestamp:      int64(w.Timestamp * 1_000_000),
			SequenceNumber: w.SequenceNumber,
		},
		ID:         w.ID,
		Exchange:   int32(w.Exchange),
		Price:      w.Price,
		Size_:      uint32(w.Size),
		Conditions: w.Conditions,
		Tape:       int32(w.Tape),
	}
}

var _ jsonTranslator[*stocks.Trade] = stocksTranslator

func stocksTranslator(buf []byte) (*stocks.Trade, error) {
	var t websocketTrade
	if err := json.Unmarshal(buf, &t); err != nil {
		return nil, err
	}

	return t.toStocksTrade(), nil
}

var _ jsonTranslator[*currencies.Trade] = currenciesTranslator

func currenciesTranslator(buf []byte) (*currencies.Trade, error) {
	var t currencies.Trade
	if err := t.UnmarshalJSON(buf); err != nil {
		return nil, err
	}

	return &t, nil
}

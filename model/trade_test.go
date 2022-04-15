package model

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type TestTradeSuite struct {
	suite.Suite
}

func (t *TestTradeSuite) SetupSuite() {

}

func TestItemIdIngestionTestSuite(t *testing.T) {
	suite.Run(t, new(TestTradeSuite))
}

func (t *TestTradeSuite) TestNewTrade() {

	type args struct {
		tm  TradeMapper
		rec []string
	}
	tests := []struct {
		name          string
		args          args
		expectedTrade *Trade
	}{
		{
			name: "Correct Mapping",
			args: args{
				tm:  NewTradeMapper(),
				rec: []string{"1", "2021-03-26T20:17:21Z", "BTCUSDT", "sell", "1.60", "109.41", "0.18", "175.73"},
			},
			expectedTrade: &Trade{
				Id:         1,
				DateTime:   mustParseTime("2021-03-26T20:17:21Z"),
				SymbolPair: "BTCUSDT",
				Type:       "sell",
				Price:      1.60,
				Amount:     109.41,
				Fee:        0.18,
				Total:      175.73,
			},
		},
	}
	for _, tt := range tests {
		gotTrade := NewTrade(tt.args.tm, tt.args.rec)
		t.Require().Equal(tt.expectedTrade, gotTrade)

	}
}

func mustParseTime(v string) time.Time {
	t, _ := time.Parse(time.RFC3339, v)
	return t
}

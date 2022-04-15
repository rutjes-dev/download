package model

import (
	"context"
	"fmt"
	"github.com/uptrace/bun"
	"reflect"
	"strconv"
	"time"
)

const tagName = "map"

type Trade struct {
	Id         int64     `bun:",pk,autoincrement" map:"id"`
	DateTime   time.Time `bun:"date_time" map:"date_time"`
	SymbolPair string    `bun:"symbol_pair" map:"symbol_pair"`
	Type       string    `bun:"type" map:"type"`
	Price      float64   `bun:"price" map:"price"`
	Amount     float64   `bun:"amount" map:"amount"`
	Fee        float64   `bun:"fee" map:"fee"`
	Total      float64   `bun:"total" map:"total"`
}

func (r Trade) Identifier() int64 {
	return r.Id
}

/* CSV Interface */
func (r Trade) Headers() []string {
	return []string{"id", "date_time", "symbol_pair", "type", "price", "amount", "fee", "total"}
}

func (r Trade) Values() []string {
	return []string{fmt.Sprintf("%d", r.Id), r.DateTime.Format(time.RFC3339), r.SymbolPair, r.Type, format(r.Price, 2), format(r.Amount, 2), format(r.Fee, 2), format(r.Total, 2)}
}

func format(value float64, decimals int) string {
	f := fmt.Sprintf("%%.%df", decimals)
	return fmt.Sprintf(f, value)
}

type TradeMapper struct {
	mapper map[string]func(string) (any, error)
}

func NewTradeMapper() TradeMapper {
	mapper := make(map[string]func(string) (any, error))
	mapper["id"] = parseInt
	mapper["date_time"] = parseTime
	mapper["symbol_pair"] = parseString
	mapper["type"] = parseString
	mapper["price"] = parseFloat
	mapper["amount"] = parseFloat
	mapper["fee"] = parseFloat
	mapper["total"] = parseFloat

	return TradeMapper{mapper}
}

func (tm TradeMapper) Map(column, value string) (any, error) {
	if mf, ok := tm.mapper[column]; ok {
		return mf(value)
	}
	return nil, fmt.Errorf("column not mapped: %s", column)
}

func NewTrade(tm TradeMapper, rec []string) (trade *Trade) {
	tr := Trade{}

	// assert same length
	if len(tr.Headers()) != len(rec) {
		return nil
	}

	record := createRecordMap(&tr, rec)

	rt := reflect.TypeOf(tr)
	rv := reflect.ValueOf(&tr).Elem()

	for i := 0; i < rt.NumField(); i++ {
		tf := rt.Field(i)
		vf := rv.Field(i)
		tag := tf.Tag.Get(tagName)
		if recValue, ok := record[tag]; ok {
			mappedValue, err := tm.Map(tag, recValue)
			if err != nil {
				return nil
			}

			kind := vf.Kind()
			switch kind {
			case reflect.Int64:
				vf.SetInt(mappedValue.(int64))
			case reflect.Float64:
				vf.SetFloat(mappedValue.(float64))
			case reflect.String:
				vf.SetString(mappedValue.(string))
			case reflect.Struct:
				vf.Set(reflect.ValueOf(mappedValue))
			}

		}

	}

	return &tr
}

func createRecordMap(trade *Trade, rec []string) map[string]string {
	record := map[string]string{}
	for i, h := range trade.Headers() {
		record[h] = rec[i]
	}
	return record
}

func parseString(v string) (any, error) {
	return v, nil
}

func parseTime(v string) (any, error) {
	return time.Parse(time.RFC3339, v)
}

func parseInt(v string) (any, error) {
	return strconv.ParseInt(v, 10, 64)
}

func parseFloat(v string) (any, error) {
	return strconv.ParseFloat(v, 64)
}

/* BUM ORM */
func (*Trade) AfterCreateTable(ctx context.Context, query *bun.CreateTableQuery) error {
	_, err := query.DB().NewCreateIndex().
		Model((*Trade)(nil)).
		Index("date_time__idx").
		Column("date_time").
		Exec(ctx)
	return err
}

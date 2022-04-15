package fixtures

import (
	"context"
	"download/datasource"
	"download/model"
	"encoding/csv"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
	"log"
	"math/rand"
	"os"
	"time"
)

var _ bun.AfterCreateTableHook = (*model.Trade)(nil)

func InitFixture(from, to time.Time, count, split int) {

	print("populating database ")
	ctx := context.Background()
	db := datasource.OpenPostgresDB()
	db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(false)))
	// Register models for the fixture.
	db.RegisterModel((*model.Trade)(nil))
	if err := db.ResetModel(ctx, (*model.Trade)(nil)); err != nil {
		panic(err)
	}

	delta := to.Sub(from).Nanoseconds() / int64(count)

	for i := 0; i < count; i++ {
		if i%1000 == 0 {
			print(".")
		}
		price := 1 + rand.Float64()
		amount := 100 + 10*rand.Float64()
		fee := price * amount * 0.001
		typeV := "buy"
		if i%2 == 0 {
			typeV = "sell"
		}
		trade := &model.Trade{
			DateTime:   from.Add(time.Duration(int64(i)*delta) * time.Nanosecond),
			SymbolPair: "BTCUSDT",
			Type:       typeV,
			Price:      price,
			Amount:     amount,
			Fee:        fee,
			Total:      price*amount + fee,
		}
		db.NewInsert().Model(trade).Exec(ctx)
	}

	var trades []*model.Trade
	db.NewSelect().Model(&trades).Where("id < ?", split).OrderExpr("id ASC").Scan(ctx)
	createMinioCsvFile(trades)
	print(". splitting data .")
	db.NewDelete().Model(&trades).WherePK().Exec(ctx)

	println(" done")
}

func createMinioCsvFile(trades []*model.Trade) {
	fi, err := os.OpenFile("./s3-data/trade-store/trade.csv", os.O_WRONLY|os.O_CREATE, 0777)
	defer fi.Close()
	if err != nil {
		log.Printf("Error in Create\n")
		panic(err)
	}
	writer := csv.NewWriter(fi)
	writer.UseCRLF = true
	for i, t := range trades {
		if i == 0 {
			writer.Write(t.Headers())
		}
		writer.Write(t.Values())
	}

	writer.Flush()
}

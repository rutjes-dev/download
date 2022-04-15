package datasource

import (
	"bytes"
	"context"
	"database/sql"
	"download/model"
	"download/ports"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"time"
)

func OpenPostgresDB() *bun.DB {
	user := "postgres"
	password := "password"
	address := "localhost:5432"
	database := "postgres"

	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, address, database)
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	return bun.NewDB(sqldb, pgdialect.New())
}

type PostgresReader struct {
	from     time.Time
	to       time.Time
	db       bun.IDB
	pageSize int
	dbCursor *model.Cursor[int64]
	buffer   *bytes.Buffer
	ctx      context.Context
}

func (p *PostgresReader) Next() ([]ports.CsvRecord, error) {
	// start situation
	if p.dbCursor == nil {
		p.dbCursor = model.NewCursor[int64](0, 0)
	}

	var entries []model.Trade

	if err := p.db.NewSelect().
		Model(&entries).
		Where("id > ?", p.dbCursor.End).
		Where("date_time >= ?", p.from).
		Where("date_time <=  ?", p.to).
		OrderExpr("id ASC").
		Limit(p.pageSize).
		Scan(p.ctx); err != nil {
		return nil, err
	}
	p.dbCursor = NewCursorFromEntries[model.Trade, int64](entries)

	models := make([]ports.CsvRecord, len(entries))
	for i := range entries {
		models[i] = entries[i]
	}
	return models, nil
}

func NewPostgresReader(ctx context.Context, db bun.IDB, pageSize int, from, to time.Time) *PostgresReader {
	return &PostgresReader{ctx: ctx, db: db, pageSize: pageSize, buffer: &bytes.Buffer{}, from: from, to: to}
}

func (p *PostgresReader) Read(data []byte) (n int, err error) {
	// is there still some data left in the buffer
	if p.buffer.Len() > 0 {
		return readFromBuffer(data, p.buffer, n, err)
	}

	p.buffer, err = createNewBuffer(p)
	if err != nil { // next could be EOF
		return 0, err
	}

	return p.buffer.Read(data)
}

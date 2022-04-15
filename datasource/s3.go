package datasource

import (
	"bytes"
	"context"
	"download/model"
	"download/ports"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"time"
)

type S3Reader struct {
	from     time.Time
	to       time.Time
	client   *s3.S3
	pageSize int
	dbCursor *model.Cursor[int64]
	buffer   *bytes.Buffer
	ctx      context.Context
	tm       model.TradeMapper
}

func OpenS3DB() *s3.S3 {
	svc := s3.New(session.Must(session.NewSession(aws.NewConfig().
		WithEndpoint("http://localhost:9000").
		WithDisableSSL(true).
		WithS3ForcePathStyle(true).
		WithCredentials(credentials.NewStaticCredentials("minio_access_key", "minio_secret_key", "")).
		WithRegion("eu-west-1"))))
	return svc
}

func NewS3Reader(ctx context.Context, client *s3.S3, pageSize int, from, to time.Time) *S3Reader {
	return &S3Reader{ctx: ctx, client: client, pageSize: pageSize, buffer: &bytes.Buffer{}, from: from, to: to, tm: model.NewTradeMapper()}
}

func (s *S3Reader) Next() ([]ports.CsvRecord, error) {
	// start situation
	if s.dbCursor == nil {
		s.dbCursor = model.NewCursor[int64](0, 0)
	}

	selectStmt := fmt.Sprintf(`SELECT * FROM S3Object s WHERE s."id" > %d AND CAST(s."date_time" AS TIMESTAMP) BETWEEN CAST('%s' AS TIMESTAMP) AND CAST('%s' AS TIMESTAMP) LIMIT %d`, s.dbCursor.End, s.from.Format(time.RFC3339), s.to.Format(time.RFC3339), s.pageSize)
	// select partial data from csv
	resp, err := s.client.SelectObjectContent(&s3.SelectObjectContentInput{
		// bucket name
		Bucket: aws.String("trade-store"),
		// csv data
		Key: aws.String("trade.csv"),
		// s3 select
		Expression: aws.String(selectStmt),
		// lets us use select
		ExpressionType: aws.String(s3.ExpressionTypeSql),
		InputSerialization: &s3.InputSerialization{
			CSV: &s3.CSVInput{
				AllowQuotedRecordDelimiter: nil,
				Comments:                   nil,
				FieldDelimiter:             nil,
				// lets us use column values in select statement
				FileHeaderInfo:       aws.String("Use"),
				QuoteCharacter:       nil,
				QuoteEscapeCharacter: nil,
				RecordDelimiter:      nil,
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{
				QuoteFields:          aws.String("ASNEEDED"),
				RecordDelimiter:      aws.String("\r\n"),
				FieldDelimiter:       aws.String(","),
				QuoteCharacter:       aws.String(`"`),
				QuoteEscapeCharacter: aws.String(`"`),
			},
		},
	})

	if err != nil {
		log.Printf("failed making API request, %v\n", err)
		return nil, err
	}
	defer resp.EventStream.Close()

	results := &bytes.Buffer{}
	for event := range resp.EventStream.Events() {
		switch e := event.(type) {
		case *s3.RecordsEvent:
			results.Write(e.Payload)
		case *s3.StatsEvent:
			//fmt.Printf("Processed %d bytes\n", *e.Details.BytesProcessed)
		case *s3.EndEvent:
			break
		}
	}

	// conversion csv -> model.Trade
	resReader := csv.NewReader(results)
	entries := make([]model.Trade, 0)
	for {
		record, err := resReader.Read()
		if err == io.EOF {
			break
		}
		trade := model.NewTrade(s.tm, record)

		if trade == nil {
			return nil, err
		}

		s.dbCursor.End = trade.Identifier()
		entries = append(entries, *trade)
	}

	s.dbCursor = NewCursorFromEntries[model.Trade, int64](entries)

	// conversion from model to interface array
	models := make([]ports.CsvRecord, len(entries))
	for i := range entries {
		models[i] = entries[i]
	}
	return models, nil
}

func (s *S3Reader) Read(data []byte) (n int, err error) {
	// is there still some data left in the buffer
	if s.buffer.Len() > 0 {
		return readFromBuffer(data, s.buffer, n, err)
	}
	// buffer empty or nil, fetch data and put in buffer
	s.buffer, err = createNewBuffer(s)
	if err != nil { // next could be EOF
		return 0, err
	}
	return s.buffer.Read(data)
}

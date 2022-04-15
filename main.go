package main

import (
	"context"
	"download/datasource"
	"download/fixtures"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

func init() {
	// create fake data from a year a go
	to := time.Now()
	from := to.AddDate(-1, 0, 0)

	totalTrades := 10000
	transferToS3 := 8000 // 1 t/m 7999 going to be stored in folder
	fixtures.InitFixture(from, to, totalTrades, transferToS3)
}

func main() {

	to := time.Now()
	from := to.AddDate(-1, 0, 0)
	from = from.Add(-1 * time.Hour)

	fetchSize := 1000

	source1 := datasource.NewS3Reader(context.Background(),
		datasource.OpenS3DB(), fetchSize, from, to)

	source2 := datasource.NewPostgresReader(context.Background(),
		datasource.OpenPostgresDB(), fetchSize, from, to)

	download := NewDownload(from, to)

	start := time.Now()
	print(fmt.Sprintf("downloading trade data from: %s to: %s",
		from.Format(time.UnixDate), to.Format(time.UnixDate)))

	downloadCsvTo(download, "test.csv", source1, source2)

	println(" in", time.Since(start).String())

}

func downloadCsvTo(download *Download, file string, source1 io.Reader, source2 io.Reader) {
	fi, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Printf("Error in Create\n")
		panic(err)
	}
	err = download.write(fi, source1, source2)
	if err != nil {
		log.Printf("Error in Create\n")
		panic(err)
	}
	fi.Close()
}




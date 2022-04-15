package main

import (
	"io"
	"time"
)

type Download struct {
	size int
	from time.Time
	to   time.Time
}

func NewDownload(from, to time.Time) *Download {
	return &Download{
		size: 1024,
		from: from,
		to:   to,
	}
}

func (d *Download) write(sink io.Writer, dataSources ...io.Reader) error {
	for _, ds := range dataSources {
		b := make([]byte, 0, d.size)
		for {
			n, err := ds.Read(b[0:d.size])
			if n > 0 {
				_, writerErr := sink.Write(b[0:n])
				if writerErr != nil {
					return writerErr
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

		}
	}
	return nil
}

package datasource

import (
	"bytes"
	"download/model"
	"download/ports"
	"download/types"
	"encoding/csv"
	"io"
)

func createNewBuffer(n ports.CsvIterator) (buf *bytes.Buffer, err error) {
	entries, err := n.Next()
	if err != nil {
		return nil, err
	}

	// create new buffer
	buf = &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	writer.UseCRLF = true

	for _, entry := range entries {
		err = writer.Write(entry.Values())
		if err != nil {
			return
		}
	}
	writer.Flush()

	return
}

func readFromBuffer(data []byte, buffer *bytes.Buffer, n int, err error) (int, error) {
	n, err = buffer.Read(data)
	if err != nil && err != io.EOF {
		return 0, err
	}
	// buffer empty, last read
	return n, nil
}

func NewCursorFromEntries[T ports.Identifiable[C], C types.Identifier](entries []T) *model.Cursor[C] {
	if len(entries) == 0 {
		return nil
	}
	return model.NewCursor[C](entries[0].Identifier(), entries[len(entries)-1].Identifier())
}

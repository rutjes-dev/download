package ports

type CsvRecord interface {
	Headers() []string
	Values() []string
}

type CsvIterator interface {
	Next() ([]CsvRecord, error)
}

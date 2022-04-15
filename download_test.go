package main

import (
	"bytes"
	"github.com/stretchr/testify/suite"
	"strings"
	"testing"
	"time"
)

type TestDownloadSuite struct {
	suite.Suite
}

func (d *TestDownloadSuite) SetupSuite() {

}

func TestItemIdIngestionTestSuite(t *testing.T) {
	suite.Run(t, new(TestDownloadSuite))
}

func (d *TestDownloadSuite) TestDownload_Write() {

	source1 := strings.NewReader("a\n")
	source2 := strings.NewReader("b\n")
	source3 := strings.NewReader("c\n")

	download := NewDownload(time.Now(), time.Now())

	sink := new(bytes.Buffer)

	err := download.write(sink, source1, source2, source3)

	d.Require().NoError(err)
	d.Require().Equal("a\nb\nc\n", sink.String())

}

package cmd

import (
	"os"
	"testing"
)

func TestIsParquetFileTrue(t *testing.T) {
	dir, _ := os.Getwd()
	fp, err := os.Open(dir + "/../test_resources/test1.parquet")
	if err != nil {
		panic(err)
	}
	if !isParquetFile(fp) {
		t.Fail()
	}
}

func TestIsParquetFileFalse(t *testing.T) {
	dir, _ := os.Getwd()
	fp, err := os.Open(dir + "/../go.sum")
	if err != nil {
		panic(err)
	}
	if isParquetFile(fp) {
		t.Fail()
	}
}

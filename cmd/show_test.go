package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToTableString1File(t *testing.T) {
	dir, _ := os.Getwd()
	filepath := dir + "/../test_resources/test1.parquet"
	actual := toTableString([]string{filepath}, TableConfigDefault)
	expected := `+-------+-----+-------+
| one   | two | three |
+-------+-----+-------+
| -1    | foo | true  |
| <nil> | bar | false |
| 2.5   | baz | true  |
+-------+-----+-------+`
	assert.Equal(t, expected, actual, "they should be equal")
}

func TestToTableString2File(t *testing.T) {
	dir, _ := os.Getwd()
	filepath1 := dir + "/../test_resources/test1.parquet"
	filepath2 := dir + "/../test_resources/test2.parquet"
	actual := toTableString([]string{filepath1, filepath2}, TableConfigDefault)
	expected := `+-------+-----+-------+
| one   | two | three |
+-------+-----+-------+
| -1    | foo | true  |
| <nil> | bar | false |
| 2.5   | baz | true  |
| -1    | foo | true  |
| <nil> | bar | false |
| 2.5   | baz | true  |
+-------+-----+-------+`
	assert.Equal(t, expected, actual, "they should be equal")
}

package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToCsvString(t *testing.T) {
	dir, _ := os.Getwd()
	filepath := dir + "/../test_resources/test1.parquet"
	actual :=	toCsvString(filepath)
	expected := `one,two,three
-1,foo,true
<nil>,bar,false
2.5,baz,true`
	assert.Equal(t, expected, actual, "they should be equal")
}

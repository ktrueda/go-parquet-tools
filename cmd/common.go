package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ktrueda/go-parquet-tools/gen-go/parquet"

	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jedib0t/go-pretty/v6/table"
)

/*
check if the file is a valid parquet file
https://parquet.apache.org/docs/file-format/
*/
func isParquetFile(fp *os.File) bool {
	//check first 4 bytes of the file
	_, err := fp.Seek(0, os.SEEK_CUR)
	check(err)
	b1 := make([]byte, 4)
	n1, err := fp.Read(b1)
	check(err)
	if string(b1[:n1]) != "PAR1" {
		return false
	}

	//cecck last 4 bytes of the file
	_, err = fp.Seek(-4, os.SEEK_END)
	check(err)
	b2 := make([]byte, 4)
	n2, err := fp.Read(b2)
	check(err)
	if string(b2[:n2]) != "PAR1" {
		return false
	}

	return true
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getFileMeta(fp *os.File) parquet.FileMetaData {
	metaSize := getFileMetaSize(fp)
	_, err := fp.Seek(int64(-1*(8+metaSize)), os.SEEK_END)
	check(err)
	b := make([]byte, metaSize)
	_, err = fp.Read(b)
	check(err)
	buff := thrift.NewTMemoryBufferLen(metaSize)
	_, err = buff.Write(b)
	check(err)

	pin := thrift.NewTCompactProtocol(buff)
	pq := parquet.FileMetaData{}
	ctx := context.Background()
	err = pq.Read(ctx, pin)
	check(err)
	return pq
}

func getFileMetaSize(fp *os.File) int {
	_, err := fp.Seek(-8, os.SEEK_END)
	check(err)
	b2 := make([]byte, 4)
	_, err = fp.Read(b2)
	check(err)
	return int(binary.LittleEndian.Uint32(b2))
}

type TableConfig struct {
	nilExpression string
}

var TableConfigDefault = TableConfig{
	nilExpression: "<nil>",
}

func readAsTable(filepath string, config TableConfig) table.Writer {

	tbl := table.NewWriter()

	rdr, err := file.OpenParquetFile(filepath, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening parquet file: ", err)
		os.Exit(1)
	}

	// show header row
	columns := table.Row{}
	numOfCol := rdr.MetaData().Schema.NumColumns()
	for c := 0; c < numOfCol; c++ {
		colName := rdr.MetaData().Schema.Column(c).Name()
		columns = append(columns, colName)
	}
	tbl.AppendHeader(columns)

	for r := 0; r < rdr.NumRowGroups(); r++ {
		rgr := rdr.RowGroup(r)
		// get dumpers
		dumpers := make([]*Dumper, numOfCol)
		for c := 0; c < numOfCol; c++ {
			col, err := rgr.Column(c)
			if err != nil {
				log.Fatalf("unable to fetch err=%s", err)
			}
			dumpers[c] = createDumper(col)
		}

		// append values
		for {
			rowVal := table.Row{}
			data := false
			for _, d := range dumpers {
				if val, ok := d.Next(); ok {
					if val == nil {
						rowVal = append(rowVal, config.nilExpression)
					} else {
						rowVal = append(rowVal, val)
					}
					data = true
				} else {
					break
				}
			}
			if !data {
				break
			}
			tbl.AppendRow(rowVal)
		}
	}
	return tbl
}

func downloadFileFromS3(s3Bucket string, s3Key string, awsProfile string) string {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           awsProfile,
		SharedConfigState: session.SharedConfigEnable,
	}))

	filePath := "/tmp/sample.parquet"

	f, err := os.Create(filePath)
	check(err)

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				fmt.Fprintf(os.Stderr, "Bucket %s does not exist\n", s3Bucket)
				os.Exit(1)
			case s3.ErrCodeNoSuchKey:
				fmt.Fprintf(os.Stderr, "s3://%s/%s does not exist\n", s3Bucket, s3Key)
				os.Exit(1)
			default:
				panic(err)
			}
		}
	}
	return filePath
}

func isS3File(filepath string) bool {
	return strings.HasPrefix(filepath, "s3://")
}

func extractS3Bucket(filepath string) string {
	return strings.Split(filepath, "/")[2]
}

func extractS3Key(filepath string) string {
	return strings.Join(strings.Split(filepath, "/")[3:], "/")
}

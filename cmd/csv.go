/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
)

type CsvOptions struct {
	nilExpression string
	skipHeader    bool
	awsProfile    string
}

var (
	csvOpt = &CsvOptions{}
)

// csvCmd represents the csv command
var csvCmd = &cobra.Command{
	Use:   "csv",
	Short: "csv [/path/to/file(s)]",
	Long:  `show contents of a parquet file in csv format`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]

		var targetFilePath []string
		if isWildCard(filePath) {
			globResult, err := filepath.Glob(filePath)
			check(err)
			targetFilePath = globResult
		} else if isS3File(filePath) {
			s3Bucket := extractS3Bucket(filePath)
			s3Key := extractS3Key(filePath)
			targetFilePath = []string{downloadFileFromS3(s3Bucket, s3Key, csvOpt.awsProfile)}
		} else {
			targetFilePath = []string{filePath}
		}

		config := TableConfig{
			nilExpression: csvOpt.nilExpression,
			skipHeader:    csvOpt.skipHeader,
		}

		csvStr := toCsvString(targetFilePath, config)
		fmt.Print(csvStr)
	},
}

func init() {
	rootCmd.AddCommand(csvCmd)
	csvCmd.Flags().StringVarP(&csvOpt.nilExpression, "nil", "n", "<nil>", "nil expression")
	csvCmd.Flags().BoolVarP(&csvOpt.skipHeader, "skipHeader", "s", false, "skip header row")
	csvCmd.Flags().StringVarP(&csvOpt.awsProfile, "awsProfile", "a", "default", "aws profile")
}

func toCsvString(filepath []string, config TableConfig) string {
	tbl := readAsTable(filepath, config)
	return tbl.RenderCSV()
}

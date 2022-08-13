/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

type CsvOptions struct {
	nilExpression string
	awsProfile    string
}

var (
	csvOpt = &CsvOptions{}
)

// csvCmd represents the csv command
var csvCmd = &cobra.Command{
	Use:   "csv",
	Short: "csv [/path/to/file]",
	Long:  `show contents of a parquet file in csv format`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filepath := args[0]

		var targetFilePath string
		if isS3File(filepath) {
			s3Bucket := extractS3Bucket(filepath)
			s3Key := extractS3Key(filepath)
			targetFilePath = downloadFileFromS3(s3Bucket, s3Key, csvOpt.awsProfile)
		} else {
			targetFilePath = filepath
		}

		config := TableConfig{}
		config.nilExpression = csvOpt.nilExpression

		csvStr := toCsvString(targetFilePath, config)
		fmt.Print(csvStr)
	},
}

func init() {
	rootCmd.AddCommand(csvCmd)
	csvCmd.Flags().StringVarP(&csvOpt.nilExpression, "nil", "n", "<nil>", "nil expression")
	csvCmd.Flags().StringVarP(&csvOpt.awsProfile, "awsProfile", "a", "default", "aws profile")
}

func toCsvString(filepath string, config TableConfig) string {
	tbl := readAsTable(filepath, config)
	return tbl.RenderCSV()
}

/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

type ShowOptions struct {
	nilExpression string
	awsProfile    string
}

var (
	showOpt = &ShowOptions{}
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "show [/path/to/file]",
	Long:  `show contents of a parquet file in table format`,
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
		config.nilExpression = showOpt.nilExpression

		tableStr := toTableString(targetFilePath, config)
		fmt.Print(tableStr)
	},
}

func init() {
	rootCmd.AddCommand(showCmd)
	showCmd.Flags().StringVarP(&showOpt.nilExpression, "nil", "n", "<nil>", "nil expression")
	showCmd.Flags().StringVarP(&csvOpt.awsProfile, "awsProfile", "a", "default", "aws profile")
}

func toTableString(filepath string, config TableConfig) string {
	tbl := readAsTable([]string{filepath}, config)
	tbl.Style().Format.Header = text.FormatDefault
	return tbl.Render()
}

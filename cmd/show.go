/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"path/filepath"

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

func toTableString(filepath []string, config TableConfig) string {
	tbl := readAsTable(filepath, config)
	tbl.Style().Format.Header = text.FormatDefault
	return tbl.Render()
}

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

		config := TableConfig{}
		config.nilExpression = csvOpt.nilExpression

		csvStr := toCsvString(filepath, config)
		fmt.Print(csvStr)
	},
}

func init() {
	rootCmd.AddCommand(csvCmd)
	csvCmd.Flags().StringVarP(&csvOpt.nilExpression, "nil", "n", "<nil>", "nil expression")
}

func toCsvString(filepath string, config TableConfig) string{
	tbl := readAsTable(filepath, config)
	return tbl.RenderCSV()
}

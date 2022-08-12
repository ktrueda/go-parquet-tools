/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// csvCmd represents the csv command
var csvCmd = &cobra.Command{
	Use:   "csv",
	Short: "csv [/path/to/file]",
	Long:  `show contents of a parquet file in csv format`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filepath := args[0]
		csvStr := toCsvString(filepath)
		fmt.Print(csvStr)
	},
}

func init() {
	rootCmd.AddCommand(csvCmd)
}

func toCsvString(filepath string) string{
	tbl := readAsTable(filepath)
	return tbl.RenderCSV()
}

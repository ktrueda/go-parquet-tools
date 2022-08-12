/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

// csvCmd represents the csv command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "show [/path/to/file]",
	Long:  `show contents of a parquet file in table format`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filepath := args[0]
		tableStr := toTableString(filepath)
		fmt.Print(tableStr)
	},
}

func init() {
	rootCmd.AddCommand(showCmd)
}

func toTableString(filepath string) string {
	tbl := readAsTable(filepath)
	tbl.Style().Format.Header = text.FormatDefault
	return tbl.Render()
}

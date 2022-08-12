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

		config := TableConfig{}
		config.nilExpression = showOpt.nilExpression


		tableStr := toTableString(filepath, config)	
		fmt.Print(tableStr)
	},
}

func init() {
	rootCmd.AddCommand(showCmd)
	showCmd.Flags().StringVarP(&showOpt.nilExpression, "nil", "n", "<nil>", "nil expression")
}

func toTableString(filepath string, config TableConfig) string {
	tbl := readAsTable(filepath, config)
	tbl.Style().Format.Header = text.FormatDefault
	return tbl.Render()
}

/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type inspectOptions struct {
	awsProfile string
}

var (
	inspectOpt = &inspectOptions{}
)

// inspectCmd represents the inspect command
var inspectCmd = &cobra.Command{
	Use:   "inspect [path/to/file]",
	Short: "inscpect parquet file meta data",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var filepath = args[0]

		var targetFilePath string
		if isS3File(filepath) {
			s3Bucket := extractS3Bucket(filepath)
			s3Key := extractS3Key(filepath)
			targetFilePath = downloadFileFromS3(s3Bucket, s3Key, csvOpt.awsProfile)
		} else {
			targetFilePath = filepath
		}

		fp, err := os.Open(targetFilePath)
		check(err)
		defer fp.Close()

		if !isParquetFile(fp) {
			fmt.Fprintln(os.Stderr, "this is NOT a parquet file")
			os.Exit(1)
		}
		fileMeta := getFileMeta(fp)

		fmt.Println("Version: ", fileMeta.Version)
		fmt.Println("Schema: ")
		for _, schema := range fileMeta.Schema {
			fmt.Printf("\t######### %s #########\n", schema.Name)
			fmt.Println("\tType: ", schema.Type)
			fmt.Println("\tTypeLength: ", schema.TypeLength)
			fmt.Println("\tRepetitionType: ", schema.RepetitionType)
			fmt.Println("\tName: ", schema.Name)
			fmt.Println("\tNumChildren: ", schema.NumChildren)
			fmt.Println("\tConvertedType: ", schema.ConvertedType)
			fmt.Println("\tScale: ", schema.Scale)
			fmt.Println("\tPrecision: ", schema.Precision)
			fmt.Println("\tFieldID: ", schema.FieldID)
			fmt.Println("\tLogicalType: ", schema.LogicalType)
		}
		fmt.Println("NumRows: ", fileMeta.NumRows)
		fmt.Println("RowGroups: ")
		for _, rowGroup := range fileMeta.RowGroups {
			fmt.Println("\tColumns: ")
			for _, column := range rowGroup.Columns {
				fmt.Println("\t\t#########")
				fmt.Println("\t\tFilePath ", column.FilePath)
				fmt.Println("\t\tFileOffset ", column.FileOffset)
				fmt.Println("\t\tMetaData.Type ", column.MetaData.Type)
				fmt.Println("\t\tMetaData.Encodings ", column.MetaData.Encodings)
				fmt.Println("\t\tMetaData.PathInSchema ", column.MetaData.PathInSchema)
				fmt.Println("\t\tMetaData.Codec ", column.MetaData.Codec)
				fmt.Println("\t\tMetaData.NumValues ", column.MetaData.NumValues)
				fmt.Println("\t\tMetaData.TotalUncompressedSize ", column.MetaData.TotalUncompressedSize)
				fmt.Println("\t\tMetaData.TotalCompressedSize ", column.MetaData.TotalCompressedSize)
				fmt.Println("\t\tMetaData.KeyValueMetadata ", column.MetaData.KeyValueMetadata)
				fmt.Println("\t\tMetaData.DataPageOffset ", column.MetaData.DataPageOffset)
				fmt.Println("\t\tMetaData.IndexPageOffset ", column.MetaData.IndexPageOffset)
				fmt.Println("\t\tMetaData.DictionaryPageOffset ", column.MetaData.DictionaryPageOffset)
				fmt.Println("\t\tMetaData.Statistics ", column.MetaData.Statistics)
				fmt.Println("\t\tMetaData.EncodingStats ", column.MetaData.EncodingStats)
				fmt.Println("\t\tMetaData.BloomFilterOffset ", column.MetaData.BloomFilterOffset)
				fmt.Println("\t\tOffsetIndexOffset ", column.OffsetIndexOffset)
				fmt.Println("\t\tOffsetIndexLength ", column.OffsetIndexLength)
				fmt.Println("\t\tColumnIndexOffset ", column.ColumnIndexOffset)
				fmt.Println("\t\tColumnIndexLength ", column.ColumnIndexLength)
				fmt.Println("\t\tCryptoMeatadata ", column.CryptoMetadata)
				fmt.Println("\t\tEncryptedColumnMetadata ", column.EncryptedColumnMetadata)
			}
			fmt.Println("\tTotalByteSize: ", rowGroup.TotalByteSize)
			fmt.Println("\tNumRows: ", rowGroup.NumRows)
			fmt.Println("\tSotringColumns: ", rowGroup.SortingColumns)
			fmt.Println("\tFileOffset: ", rowGroup.FileOffset)
			fmt.Println("\tTotalCompressedSize: ", rowGroup.TotalCompressedSize)
			fmt.Println("\tOrdinal: ", rowGroup.Ordinal)
		}
		fmt.Println("KeyValueMetaData: ", fileMeta.KeyValueMetadata)
		fmt.Println("CreatedBy: ", fileMeta.CreatedBy)
		fmt.Println("ColumnOrders: ", fileMeta.ColumnOrders)
		fmt.Println("EncryptionAlgorithm: ", fileMeta.EncryptionAlgorithm)
		fmt.Println("FooterSigningKeyMetadata: ", fileMeta.FooterSigningKeyMetadata)
	},
}

func init() {
	rootCmd.AddCommand(inspectCmd)
	inspectCmd.Flags().StringVarP(&inspectOpt.awsProfile, "awsProfile", "a", "default", "aws profile")
}

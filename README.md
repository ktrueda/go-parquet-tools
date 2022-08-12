# go-parquet-tools

Alternative to [pypi parquet-tools](https://pypi.org/project/parquet-tools/) in Golang.

## Install 

```bash
git clone git@github.com:ktrueda/go-parquet-tools.git
go install
```

## Usage

```bash
go-parquet-tools csv test_resources/test1.parquet
one,two,three
-1,foo,true
<nil>,bar,false
2.5,baz,true
```

```bash
go-parquet-tools show test_resources/test1.parquet
+-------+-----+-------+
| one   | two | three |
+-------+-----+-------+
| -1    | foo | true  |
| <nil> | bar | false |
| 2.5   | baz | true  |
+-------+-----+-------+
```

```bash
go-parquet-tools inspect test_resources/test1.parquet
```
<details>
<summary>insepct output</summary>

```bash
Version:  1
Schema:
        ######### schema #########
        Type:  <nil>
        TypeLength:  <nil>
        RepetitionType:  REQUIRED
        Name:  schema
        NumChildren:  0xc0000288d8
        ConvertedType:  <nil>
        Scale:  <nil>
        Precision:  <nil>
        FieldID:  <nil>
        LogicalType:  <nil>
        ######### one #########
        Type:  DOUBLE
        TypeLength:  <nil>
        RepetitionType:  OPTIONAL
        Name:  one
        NumChildren:  <nil>
        ConvertedType:  <nil>
        Scale:  <nil>
        Precision:  <nil>
        FieldID:  <nil>
        LogicalType:  <nil>
        ######### two #########
        Type:  BYTE_ARRAY
        TypeLength:  <nil>
        RepetitionType:  OPTIONAL
        Name:  two
        NumChildren:  <nil>
        ConvertedType:  UTF8
        Scale:  <nil>
        Precision:  <nil>
        FieldID:  <nil>
        LogicalType:  LogicalType({STRING:StringType({}) MAP:<nil> LIST:<nil> ENUM:<nil> DECIMAL:<nil> DATE:<nil> TIME:<nil> TIMESTAMP:<nil> INTEGER:<nil> UNKNOWN:<nil> JSON:<nil> BSON:<nil> UUID:<nil>})
        ######### three #########
        Type:  BOOLEAN
        TypeLength:  <nil>
        RepetitionType:  OPTIONAL
        Name:  three
        NumChildren:  <nil>
        ConvertedType:  <nil>
        Scale:  <nil>
        Precision:  <nil>
        FieldID:  <nil>
        LogicalType:  <nil>
NumRows:  3
RowGroups:
        Columns:
                #########
                FilePath  <nil>
                FileOffset  108
                MetaData.Type  DOUBLE
                MetaData.Encodings  [PLAIN_DICTIONARY PLAIN RLE]
                MetaData.PathInSchema  [one]
                MetaData.Codec  SNAPPY
                MetaData.NumValues  3
                MetaData.TotalUncompressedSize  100
                MetaData.TotalCompressedSize  104
                MetaData.KeyValueMetadata  []
                MetaData.DataPageOffset  36
                MetaData.IndexPageOffset  <nil>
                MetaData.DictionaryPageOffset  0xc000028930
                MetaData.Statistics  Statistics({Max:[0 0 0 0 0 0 4 64] Min:[0 0 0 0 0 0 240 191] NullCount:0xc000028938 DistinctCount:<nil> MaxValue:[0 0 0 0 0 0 4 64] MinValue:[0 0 0 0 0 0 240 191]})
                MetaData.EncodingStats  [PageEncodingStats({PageType:DICTIONARY_PAGE Encoding:PLAIN_DICTIONARY Count:1}) PageEncodingStats({PageType:DATA_PAGE Encoding:PLAIN_DICTIONARY Count:1})]
                MetaData.BloomFilterOffset  <nil>
                OffsetIndexOffset  <nil>
                OffsetIndexLength  <nil>
                ColumnIndexOffset  <nil>
                ColumnIndexLength  <nil>
                CryptoMeatadata  <nil>
                EncryptedColumnMetadata  []
                #########
                FilePath  <nil>
                FileOffset  281
                MetaData.Type  BYTE_ARRAY
                MetaData.Encodings  [PLAIN_DICTIONARY PLAIN RLE]
                MetaData.PathInSchema  [two]
                MetaData.Codec  SNAPPY
                MetaData.NumValues  3
                MetaData.TotalUncompressedSize  76
                MetaData.TotalCompressedSize  80
                MetaData.KeyValueMetadata  []
                MetaData.DataPageOffset  238
                MetaData.IndexPageOffset  <nil>
                MetaData.DictionaryPageOffset  0xc000028948
                MetaData.Statistics  Statistics({Max:[] Min:[] NullCount:0xc000028950 DistinctCount:<nil> MaxValue:[102 111 111] MinValue:[98 97 114]})
                MetaData.EncodingStats  [PageEncodingStats({PageType:DICTIONARY_PAGE Encoding:PLAIN_DICTIONARY Count:1}) PageEncodingStats({PageType:DATA_PAGE Encoding:PLAIN_DICTIONARY Count:1})]
                MetaData.BloomFilterOffset  <nil>
                OffsetIndexOffset  <nil>
                OffsetIndexLength  <nil>
                ColumnIndexOffset  <nil>
                ColumnIndexLength  <nil>
                CryptoMeatadata  <nil>
                EncryptedColumnMetadata  []
                #########
                FilePath  <nil>
                FileOffset  388
                MetaData.Type  BOOLEAN
                MetaData.Encodings  [PLAIN RLE]
                MetaData.PathInSchema  [three]
                MetaData.Codec  SNAPPY
                MetaData.NumValues  3
                MetaData.TotalUncompressedSize  40
                MetaData.TotalCompressedSize  42
                MetaData.KeyValueMetadata  []
                MetaData.DataPageOffset  346
                MetaData.IndexPageOffset  <nil>
                MetaData.DictionaryPageOffset  <nil>
                MetaData.Statistics  Statistics({Max:[1] Min:[0] NullCount:0xc000028970 DistinctCount:<nil> MaxValue:[1] MinValue:[0]})
                MetaData.EncodingStats  [PageEncodingStats({PageType:DATA_PAGE Encoding:PLAIN Count:1})]
                MetaData.BloomFilterOffset  <nil>
                OffsetIndexOffset  <nil>
                OffsetIndexLength  <nil>
                ColumnIndexOffset  <nil>
                ColumnIndexLength  <nil>
                CryptoMeatadata  <nil>
                EncryptedColumnMetadata  []
        TotalByteSize:  226
        NumRows:  3
        SotringColumns:  []
        FileOffset:  0xc000028978
        TotalCompressedSize:  0xc000028980
        Ordinal:  0xc000028988
KeyValueMetaData:  [KeyValue({Key:pandas Value:0xc000063330}) KeyValue({Key:ARROW:schema Value:0xc000063340})]
CreatedBy:  0xc000063350
ColumnOrders:  [ColumnOrder({TYPE_ORDER:TypeDefinedOrder({})}) ColumnOrder({TYPE_ORDER:TypeDefinedOrder({})}) ColumnOrder({TYPE_ORDER:TypeDefinedOrder({})})]
EncryptionAlgorithm:  <nil>
FooterSigningKeyMetadata:  []
```

</details>


## Benchmark result

go-parquet-tools is 100x faster than pypi parquet-tools.


| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `parquet-tools csv test_resources/test1.parquet` | 702.8 ± 19.9 | 676.2 | 739.4 | 1.00 |
| `go-parquet-tools csv test_resources/test1.parquet` | 6.6 ± 0.4 | 6.2 | 7.3 | 1.00 |


https://github.com/sharkdp/hyperfine
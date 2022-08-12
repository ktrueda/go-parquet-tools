# go-parquet-tools

Alternative to [pypi parquet-tools](https://pypi.org/project/parquet-tools/) in Golang.

## Install 

```bash
git clone git@github.com:ktrueda/go-parquet-tools.git
go install
```

## Usage

```bash
go-parquet-tools show /path/to/parquet-tools
```


## Benchmark result

go-parquet-tools is 100x faster than pypi parquet-tools.


| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `parquet-tools csv test_resources/test1.parquet` | 702.8 ± 19.9 | 676.2 | 739.4 | 1.00 |
| `go-parquet-tools csv test_resources/test1.parquet` | 6.6 ± 0.4 | 6.2 | 7.3 | 1.00 |


https://github.com/sharkdp/hyperfine
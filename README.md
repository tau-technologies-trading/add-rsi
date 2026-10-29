# add-rsi

Calculate continuous Wilder RSI columns directly in per-ticker DuckDB kline databases. Run this after importing raw data with `csv-to-duckdb`.

## Usage

```sh
cargo run --release -- --db ../db/BTCUSDT/BTCUSDT.duckdb
cargo run --release -- --db ../db/BTCUSDT/BTCUSDT.duckdb --window 14,64,256
```

The input table defaults to `klines` and must contain `open_time` and `close`. Use `--table` to choose another table. Windows map in order to `rsi_1`, `rsi_2`, and so on; defaults are 16, 64, and 256. Missing columns are added. RSI is NULL until enough price changes have been observed.

Each run reads all prices in timestamp order to reconstruct the exact Wilder state. This also accounts for historical backfills and changed prices. Only RSI values that differ are updated. Each database is updated in a transaction, so a failed calculation rolls back its changes.

This version works with DuckDB databases rather than CSV files. It does not import data, fill gaps, or download files.

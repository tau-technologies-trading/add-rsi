# add-rsi

Library for calculating continuous Wilder RSI columns in DuckDB kline tables.

## Use

Add the crate to your Rust project:

```toml
add-rsi = "1.1.2"
```

Call `process_database` with a database path, table name, and RSI windows:

```rust,ignore
add_rsi::process_database(
    std::path::Path::new("db/BTCUSDT.duckdb"),
    "klines",
    &[16, 64, 256],
)?;
```

Windows map in order to `rsi_1`, `rsi_2`, and so on. The table must contain `open_time` as its primary key and a `close` column. Missing RSI columns are added. RSI is NULL until enough price changes have been observed.

Each call reads prices in timestamp order to reconstruct the exact Wilder state, accounting for historical backfills and changed prices. Only changed RSI values are updated. Database updates run in a transaction, so a failed calculation rolls back its changes.

`inspect_database` reports the latest `open_time` and whether each configured RSI column has values at its first computable row and latest row. The literal first row is part of the warm-up period and has a NULL RSI.

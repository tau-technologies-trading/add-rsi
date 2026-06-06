# add-rsi

`add-rsi` is a 🌩️ fast Rust CLI that appends Wilder RSI columns to Binance Vision format CSV files.

The tool reads kline-style CSV rows, preserves the configured leading columns, appends one RSI column per requested window, and writes either in place or to sibling `.rsi.csv` files.

## Build

```sh
cargo build --release
```

The optimized binary is written to:

```text
target/release/add-rsi
```

## Data Layout

By default, the CLI expects BTCUSDT data here:

```text
../data/BTCUSDT/
  BTCUSDT-1s-2024-01.csv
  BTCUSDT-1s-2024-02.csv
```

With `--all`, the default root is `../data/`. The CLI searches recursively and uses each CSV file's parent directory name as the required filename prefix:

```text
../data/spot/BTCUSDT/BTCUSDT-1s-2024-01.csv
../data/spot/ETHUSDT/ETHUSDT-1s-2024-01.csv
```

Files such as `BTCUSDT-1s-2024-01.rsi.csv` are skipped because they do not match the input filename format.

## Usage

Process the default BTCUSDT directory:

```sh
add-rsi
```

Process a specific symbol directory:

```sh
add-rsi -d ../data/ETHUSDT -n ETHUSDT
```

Process all matching symbol directories recursively under `../data/`:

```sh
add-rsi --all
```

Write `.rsi.csv` copies instead of overwriting inputs:

```sh
add-rsi --copy
```

Process with RSI carryover across monthly files:

```sh
add-rsi --carry
```

Process all symbol groups with carryover per group:

```sh
add-rsi --all --carry --jobs 5
```

## Important Flags

```text
-d, --dir <DIR>          Directory to process
-n, --name <NAME>        File prefix in normal mode [default: BTCUSDT]
-i, --interval <INT>     Interval in filenames [default: 1s]
-c, --column <N>         Leading columns to preserve [default: 12]
-w, --window <N,...>     RSI windows [default: 16,64,256]
-f, --fallback <VALUE>   Value for unavailable RSI cells [default: empty]
--carry                  Carry RSI state across files in chronological order
--no-carry               Process each file independently
-j, --jobs <N>           Maximum parallel files or groups [default: 5]
-a, --auto               Skip files that already have requested RSI columns
--all                    Recursively process matching CSVs by parent directory
-o, --overwrite          Overwrite original files in place
-C, --copy               Write sibling .rsi.csv files
```

## Carry Behavior

Without `--carry`, every file is processed independently and files run in parallel.

With `--carry`, files in the same parent directory are processed in chronological order so RSI state continues across file boundaries. When `--all --carry` is used, each parent directory is processed as an independent group, and different groups run in parallel.

## Safety

By default, the CLI overwrites input CSV files after confirmation. Use `--copy` to keep originals unchanged.

Set `ADD_RSI_NO_CONFIRM=1` to skip the confirmation prompt.

## Validation

Common development checks:

```sh
cargo fmt
cargo test
cargo clippy --all-targets --all-features
```

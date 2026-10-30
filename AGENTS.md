# AGENTS.md

## Project overview

`add-rsi` is a library crate for recalculating Wilder RSI columns in DuckDB kline databases. The public API is in `src/lib.rs`; this package has no CLI executable.

## Development

- Keep the public API focused on RSI processing.
- Preserve database transaction behavior and exact chronological Wilder calculations.
- Avoid generated artifacts under `target/`.

## Validation

- `cargo fmt --check`
- `cargo build --release`
- `cargo test` when the user asks for tests or verification

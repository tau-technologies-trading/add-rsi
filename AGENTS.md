# AGENTS.md

## Project Overview

`add-rsi` is a Rust CLI that appends Wilder RSI columns to Binance Vision CSV files. The main implementation lives in `src/main.rs`.

## Common Commands

- `cargo fmt` formats Rust code.
- `cargo test` runs the test suite.
- `cargo clippy --all-targets --all-features` checks for common Rust issues.
- `cargo build --release` builds the optimized CLI binary.

## Development Notes

- Keep changes small and focused; this project is currently a single-binary crate.
- Prefer preserving existing CLI behavior unless the task explicitly requests a behavior change.
- Update `Cargo.toml` and `Cargo.lock` versions only for user-facing releases or packaged behavior changes, not for documentation-only updates.
- Avoid committing generated build artifacts under `target/`.

## Validation

Before committing code changes, run `cargo fmt`, `cargo test`, and `cargo clippy --all-targets --all-features` when relevant. For documentation-only changes, a git diff review is usually sufficient.

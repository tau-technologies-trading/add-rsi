use std::io::IsTerminal;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use duckdb::{Connection, appender_params_from_iter, types::ToSql};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug)]
struct RsiState {
    window: usize,
    last: Option<f64>,
    count: usize,
    gain: f64,
    loss: f64,
}

impl RsiState {
    fn new(window: usize) -> Self {
        Self {
            window,
            last: None,
            count: 0,
            gain: 0.0,
            loss: 0.0,
        }
    }
    fn next(&mut self, close: f64) -> Option<f64> {
        let last = self.last.replace(close)?;
        let change = close - last;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        if self.count < self.window {
            self.count += 1;
            self.gain += gain;
            self.loss += loss;
            if self.count < self.window {
                return None;
            }
            self.gain /= self.window as f64;
            self.loss /= self.window as f64;
        } else {
            let window = self.window as f64;
            self.gain = (self.gain * (window - 1.0) + gain) / window;
            self.loss = (self.loss * (window - 1.0) + loss) / window;
        }
        Some(if self.loss == 0.0 {
            if self.gain == 0.0 { 50.0 } else { 100.0 }
        } else {
            100.0 - 100.0 / (1.0 + self.gain / self.loss)
        })
    }
}

fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// RSI and timestamp state inspected before or after an import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseStatus {
    pub max_open_time: Option<i64>,
    pub rsi_at_boundaries: bool,
}

/// Inspect whether every configured RSI column has values at its first
/// computable row and at the latest row. The literal first row is a warm-up
/// row and correctly has NULL RSI values.
pub fn inspect_database(path: &Path, table: &str, windows: &[usize]) -> Result<DatabaseStatus> {
    if windows.is_empty() || windows.contains(&0) {
        bail!("RSI windows must be greater than zero");
    }
    if !path.is_file() {
        return Ok(DatabaseStatus {
            max_open_time: None,
            rsi_at_boundaries: false,
        });
    }
    let conn = Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let table_name = table.to_string();
    let quoted_table = ident(table);
    let schema_sql = format!(
        "SELECT name FROM pragma_table_info('{}')",
        table_name.replace('\'', "''")
    );
    let mut schema = conn.prepare(&schema_sql)?;
    let columns = schema
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    drop(schema);
    if columns.is_empty() {
        return Ok(DatabaseStatus {
            max_open_time: None,
            rsi_at_boundaries: false,
        });
    }
    if !columns.iter().any(|name| name == "open_time") {
        bail!("{table_name} does not contain open_time");
    }

    let max_open_time = conn.query_row(
        &format!("SELECT MAX(open_time) FROM {quoted_table}"),
        [],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    let row_count = conn.query_row(&format!("SELECT COUNT(*) FROM {quoted_table}"), [], |row| {
        row.get::<_, i64>(0)
    })? as usize;
    let has_rsi_columns =
        (1..=windows.len()).all(|index| columns.iter().any(|name| name == &format!("rsi_{index}")));
    if !has_rsi_columns || row_count == 0 {
        return Ok(DatabaseStatus {
            max_open_time,
            rsi_at_boundaries: false,
        });
    }

    let mut first_ready = true;
    for (index, window) in windows.iter().enumerate() {
        if row_count <= *window {
            first_ready = false;
            break;
        }
        let value = conn.query_row(
            &format!(
                "SELECT rsi_{} FROM {quoted_table} ORDER BY open_time LIMIT 1 OFFSET {window}",
                index + 1
            ),
            [],
            |row| row.get::<_, Option<f64>>(0),
        )?;
        if value.is_none() {
            first_ready = false;
            break;
        }
    }

    let last_columns = (1..=windows.len())
        .map(|index| format!("rsi_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let last_values = conn.query_row(
        &format!("SELECT {last_columns} FROM {quoted_table} ORDER BY open_time DESC LIMIT 1"),
        [],
        |row| {
            (0..windows.len())
                .map(|index| row.get::<_, Option<f64>>(index))
                .collect::<duckdb::Result<Vec<_>>>()
        },
    )?;

    Ok(DatabaseStatus {
        max_open_time,
        rsi_at_boundaries: first_ready && last_values.iter().all(Option::is_some),
    })
}

/// Recalculate Wilder RSI columns in a DuckDB table.
///
/// The table must have `open_time` as its primary key and a `close` column.
/// RSI columns are written as `rsi_1`, `rsi_2`, and so on, in the order of
/// `windows`.
pub fn process_database(path: &Path, table: &str, windows: &[usize]) -> Result<usize> {
    if !path.is_file() {
        bail!("database does not exist: {}", path.display());
    }
    let conn = Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let progress = if std::io::stderr().is_terminal() {
        let progress = ProgressBar::new_spinner();
        progress.set_message("Counting rows");
        progress.enable_steady_tick(Duration::from_millis(100));
        progress
    } else {
        ProgressBar::hidden()
    };
    let table_name = table.to_string();
    let table = ident(table);
    conn.execute_batch("BEGIN TRANSACTION")?;
    let result = (|| -> Result<usize> {
        // Confirm the input table exists before adding columns.
        conn.prepare(&format!("SELECT open_time, close FROM {table} LIMIT 0"))?;
        let schema_sql = format!(
            "SELECT name, pk FROM pragma_table_info('{}')",
            table_name.replace('\'', "''")
        );
        let mut schema = conn.prepare(&schema_sql)?;
        let columns = schema
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })?
            .collect::<duckdb::Result<Vec<_>>>()?;
        if !columns.iter().any(|(name, pk)| name == "open_time" && *pk)
            || columns.iter().any(|(name, pk)| name != "open_time" && *pk)
        {
            bail!("{table_name}.open_time must be a primary key");
        }
        for (name, _) in &columns {
            if let Some(suffix) = name.strip_prefix("rsi_") {
                let index = suffix.parse::<usize>().unwrap_or(0);
                if index == 0 || index > windows.len() {
                    bail!(
                        "{table_name} contains {name}; provide a matching window for every existing RSI column"
                    );
                }
            }
        }
        drop(schema);
        for i in 1..=windows.len() {
            conn.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS rsi_{i} DOUBLE"
            ))?;
        }
        let columns = (1..=windows.len())
            .map(|i| format!("rsi_{i}"))
            .collect::<Vec<_>>();
        let stage_columns = columns
            .iter()
            .map(|c| format!("{c} DOUBLE"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE __add_rsi_stage (open_time BIGINT PRIMARY KEY, {stage_columns})"
        ))?;
        if !progress.is_hidden() {
            let total: u64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            progress.disable_steady_tick();
            progress.set_length(total);
            progress.set_style(ProgressStyle::with_template(
                "RSI [{bar:40.cyan/blue}] {pos}/{len} rows ({percent}%, {per_sec}, ETA {eta})",
            )?);
        }
        let mut states = windows
            .iter()
            .copied()
            .map(RsiState::new)
            .collect::<Vec<_>>();
        let mut last_time: Option<i64> = None;
        let mut count = 0;
        {
            let mut stmt = conn.prepare(&format!(
                "SELECT open_time, close FROM {table} ORDER BY open_time"
            ))?;
            let mut rows = stmt.query([])?;
            let mut appender = conn.appender("__add_rsi_stage")?;
            let mut values = Vec::with_capacity(states.len());
            while let Some(row) = rows.next()? {
                let time: i64 = row.get(0)?;
                let close: f64 = row.get(1)?;
                if !close.is_finite() {
                    bail!("non-finite close at {time}");
                }
                if last_time.is_some_and(|prev| time <= prev) {
                    bail!("open_time must be strictly increasing at {time}");
                }
                last_time = Some(time);
                values.clear();
                values.extend(states.iter_mut().map(|state| state.next(close)));
                if values.iter().flatten().any(|value| !value.is_finite()) {
                    bail!("RSI calculation overflow at {time}");
                }
                let refs = std::iter::once(&time as &dyn ToSql)
                    .chain(values.iter().map(|value| value as &dyn ToSql));
                appender.append_row(appender_params_from_iter(refs))?;
                count += 1;
                if count % 8192 == 0 {
                    progress.set_position(count as u64);
                }
            }
            appender.flush()?;
        }
        progress.set_position(count as u64);
        if !progress.is_hidden() {
            progress.set_style(ProgressStyle::with_template("{spinner} {msg}")?);
            progress.set_message("Applying RSI values to database");
            progress.enable_steady_tick(Duration::from_millis(100));
        }
        let assignments = columns
            .iter()
            .map(|c| format!("{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let changed = columns
            .iter()
            .map(|c| format!("t.{c} IS DISTINCT FROM s.{c}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        conn.execute_batch(&format!("UPDATE {table} AS t SET {assignments} FROM __add_rsi_stage AS s WHERE t.open_time = s.open_time AND ({changed})"))?;
        conn.execute_batch("DROP TABLE __add_rsi_stage")?;
        Ok(count)
    })();
    match result {
        Ok(count) => {
            conn.execute_batch("COMMIT")?;
            progress.finish_and_clear();
            Ok(count)
        }
        Err(err) => {
            progress.finish_and_clear();
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    #[test]
    fn wilder_seed_and_smoothing() {
        let mut rsi = RsiState::new(2);
        assert_eq!(rsi.next(10.0), None);
        assert_eq!(rsi.next(11.0), None);
        assert_eq!(rsi.next(10.0), Some(50.0));
        assert_eq!(rsi.next(11.0), Some(75.0));
    }

    #[test]
    fn updates_database_and_recomputes_after_backfill() {
        let path = std::env::temp_dir().join(format!(
            "add-rsi-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE klines(open_time BIGINT PRIMARY KEY, close DOUBLE NOT NULL); INSERT INTO klines VALUES (1,10),(2,11),(4,10)").unwrap();
        drop(conn);
        assert_eq!(process_database(&path, "klines", &[2]).unwrap(), 3);
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT rsi_1 FROM klines WHERE open_time=4", [], |r| r
                .get::<_, f64>(0))
                .unwrap(),
            50.0
        );
        conn.execute_batch("INSERT INTO klines(open_time, close) VALUES (3,12)")
            .unwrap();
        drop(conn);
        assert_eq!(process_database(&path, "klines", &[2]).unwrap(), 4);
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT rsi_1 FROM klines WHERE open_time=4", [], |r| r
                .get::<_, f64>(0))
                .unwrap(),
            33.33333333333333
        );
        drop(conn);
        fs::remove_file(path).unwrap();
    }
}

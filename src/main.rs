use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;

const CLOSE_COLUMN_INDEX: usize = 4;
static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Parser, Debug)]
#[command(
    about = "Add Wilder RSI columns to Binance Vision CSV files",
    long_about = "Add Wilder RSI columns to Binance Vision CSV files.\n\nBy default this processes files independently in parallel, overwriting the original CSVs after confirmation. Use --carry for exact RSI continuity across month/file boundaries. Use --copy if you want sibling .rsi.csv outputs instead of replacing the originals.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  add-rsi -d ../data\n  add-rsi -d ../data --jobs 3\n  add-rsi -d ../data --carry --jobs 5\n  add-rsi -d ../data --copy --window 14,64,256\n\nNotes:\n  --jobs is a maximum; the program uses fewer workers if fewer files exist.\n  --carry does a carry-state pre-pass, then processes files in parallel.\n  Set ADD_RSI_NO_CONFIRM=1 to skip the confirmation prompt."
)]
struct Cli {
    /// Directory containing the CSV files to process.
    #[arg(short = 'd', long = "dir")]
    dir: PathBuf,

    /// File name prefix (e.g. SOLUSDT).
    #[arg(short = 'n', long = "name", default_value = "SOLUSDT")]
    name: String,

    /// Time interval in seconds (e.g. 1s, 5m, 1h).
    #[arg(short = 'i', long = "interval", default_value = "1s")]
    interval: String,

    /// Number of leading columns to preserve before appending RSI values.
    #[arg(short = 'c', long = "column", default_value_t = 12)]
    column: usize,

    /// Comma-separated Wilder RSI windows.
    #[arg(
        short = 'w',
        long = "window",
        value_delimiter = ',',
        num_args = 1..,
        default_values_t = [16_usize, 64, 256]
    )]
    windows: Vec<usize>,

    /// Value written when a specific RSI cell cannot be calculated.
    #[arg(short = 'f', long = "fallback", default_value = "")]
    fallback: String,

    /// Process each file independently without RSI state carryover. This is the default.
    #[arg(short = 'r', long = "no-carry")]
    no_carry: bool,

    /// Process files sequentially with exact Wilder RSI state carryover across file boundaries.
    #[arg(long = "carry", conflicts_with = "no_carry")]
    carry: bool,

    /// Maximum number of files to process in parallel during row counting and output processing.
    #[arg(short = 'j', long = "jobs", default_value_t = 5)]
    jobs: usize,

    /// Explicitly overwrite the original files in place.
    #[arg(short = 'o', long = "overwrite", conflicts_with = "copy")]
    overwrite: bool,

    /// Write processed sibling files with a .rsi.csv suffix.
    #[arg(short = 'C', long = "copy", conflicts_with = "overwrite")]
    copy: bool,

    /// Print version information and exit.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version_flag: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputMode {
    Overwrite,
    Copy,
}

#[derive(Clone, Debug)]
struct InputFile {
    path: PathBuf,
    year: u32,
    month: u32,
}

#[derive(Clone, Debug)]
struct RsiState {
    window: usize,
    last_close: Option<f64>,
    seed_count: usize,
    seed_gain_sum: f64,
    seed_loss_sum: f64,
    average_gain: Option<f64>,
    average_loss: Option<f64>,
}

impl RsiState {
    fn new(window: usize) -> Self {
        Self {
            window,
            last_close: None,
            seed_count: 0,
            seed_gain_sum: 0.0,
            seed_loss_sum: 0.0,
            average_gain: None,
            average_loss: None,
        }
    }

    fn next_cell(&mut self, close: Option<f64>, fallback: &str) -> String {
        let Some(close) = close.filter(|value| value.is_finite()) else {
            return fallback.to_owned();
        };

        let Some(previous_close) = self.last_close else {
            self.last_close = Some(close);
            return fallback.to_owned();
        };

        self.last_close = Some(close);

        let change = close - previous_close;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);

        let (next_average_gain, next_average_loss) = match (self.average_gain, self.average_loss) {
            (Some(average_gain), Some(average_loss)) => {
                let window = self.window as f64;
                (
                    ((average_gain * (window - 1.0)) + gain) / window,
                    ((average_loss * (window - 1.0)) + loss) / window,
                )
            }
            _ => {
                self.seed_count += 1;
                self.seed_gain_sum += gain;
                self.seed_loss_sum += loss;

                if self.seed_count < self.window {
                    return fallback.to_owned();
                }

                let window = self.window as f64;
                (self.seed_gain_sum / window, self.seed_loss_sum / window)
            }
        };

        self.average_gain = Some(next_average_gain);
        self.average_loss = Some(next_average_loss);

        format_rsi(next_average_gain, next_average_loss, fallback)
    }
}

struct ProgressUi {
    multi: MultiProgress,
    total_bar: ProgressBar,
    file_style: ProgressStyle,
}

struct ProgressSlot {
    file_bar: ProgressBar,
    total_bar: ProgressBar,
    finished: bool,
}

impl ProgressUi {
    fn new(stage: impl Into<String>, total_rows: u64, parallel_files: usize) -> Arc<Self> {
        let multi = MultiProgress::new();

        let total_bar = multi.add(ProgressBar::new(total_rows));
        total_bar.set_prefix(stage.into());
        total_bar.set_message(parallel_summary(parallel_files));
        total_bar.set_style(
            ProgressStyle::with_template(
                "{prefix:<7} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:>3}% {pos}/{len} rows ({per_sec}, ETA {eta}) {msg}",
            )
            .expect("valid total progress-bar template")
            .progress_chars("##-"),
        );

        let file_style = ProgressStyle::with_template(
            "  {msg:<34} [{bar:28.green/black}] {percent:>3}% {pos}/{len}",
        )
        .expect("valid file progress-bar template")
        .progress_chars("##-");

        Arc::new(Self {
            multi,
            total_bar,
            file_style,
        })
    }

    fn acquire_slot(self: &Arc<Self>, name: &str, total: u64) -> ProgressSlot {
        let file_bar = self.multi.add(ProgressBar::new(total));
        file_bar.set_style(self.file_style.clone());
        file_bar.set_message(truncate_progress_name(name));

        ProgressSlot {
            file_bar,
            total_bar: self.total_bar.clone(),
            finished: false,
        }
    }

    fn finish(&self) {
        self.total_bar.finish_and_clear();
    }
}

impl ProgressSlot {
    fn inc(&self, amount: u64) {
        self.file_bar.inc(amount);
        self.total_bar.inc(amount);
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }

        self.file_bar.finish_and_clear();
        self.finished = true;
    }
}

impl Drop for ProgressSlot {
    fn drop(&mut self) {
        self.finish();
    }
}

fn truncate_progress_name(file_name: &str) -> String {
    const MAX_LEN: usize = 34;
    if file_name.chars().count() <= MAX_LEN {
        return file_name.to_owned();
    }

    let mut shortened = file_name.chars().take(MAX_LEN - 1).collect::<String>();
    shortened.push('…');
    shortened
}
fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    if !cli.dir.is_dir() {
        bail!("{} is not a directory", cli.dir.display());
    }

    validate_windows(&cli.windows)?;

    let files = collect_input_files(&cli.dir, &cli.name, &cli.interval)?;
    if files.is_empty() {
        bail!(
            "no matching CSV files found in {} for prefix {}-{}",
            cli.dir.display(),
            cli.name,
            cli.interval
        );
    }

    let mode = match (cli.overwrite, cli.copy) {
        (_, true) => OutputMode::Copy,
        _ => OutputMode::Overwrite,
    };

    let file_info = list_directory_and_confirm(&cli.dir, &files, mode, cli.jobs)?;
    let total_rows: usize = file_info.iter().map(|(_, row_count)| *row_count).sum();

    if cli.carry {
        let parallel_files = parallel_file_count(file_info.len(), cli.jobs);
        println!(
            "\nCarryover mode: computing exact per-file RSI start states, then processing {} in parallel.",
            parallel_summary(parallel_files)
        );

        let carry_progress = ProgressUi::new("CARRY ", total_rows as u64, 1);
        let start_states = compute_start_states(&file_info, &cli.windows, &cli.fallback, &carry_progress)?;
        carry_progress.finish();

        let process_progress = ProgressUi::new("TOTAL ", total_rows as u64, parallel_files);
        let fallback = cli.fallback.clone();
        let column = cli.column;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_files)
            .build()
            .context("failed to build Rayon thread pool")?;

        let result = pool.install(|| {
            file_info
                .par_iter()
                .zip(start_states.into_par_iter())
                .try_for_each(|((file, row_count), mut states)| {
                    let name = file
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("?");
                    let slot = process_progress.acquire_slot(name, *row_count as u64);

                    process_file(
                        &file.path,
                        column,
                        &fallback,
                        &mut states,
                        mode,
                        &slot,
                    )
                })
        });

        process_progress.finish();
        result?;
    } else {
        let parallel_files = parallel_file_count(file_info.len(), cli.jobs);
        println!(
            "\nNo-carry mode: processing {} in parallel.",
            parallel_summary(parallel_files)
        );

        let process_progress = ProgressUi::new("TOTAL ", total_rows as u64, parallel_files);
        let windows = cli.windows.clone();
        let fallback = cli.fallback.clone();
        let column = cli.column;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_files)
            .build()
            .context("failed to build Rayon thread pool")?;

        let result = pool.install(|| {
            file_info
                .par_iter()
                .try_for_each(|(file, row_count)| {
                    let name = file
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("?");
                    let slot = process_progress.acquire_slot(name, *row_count as u64);
                    let mut states = windows
                        .iter()
                        .copied()
                        .map(RsiState::new)
                        .collect::<Vec<_>>();

                    process_file(
                        &file.path,
                        column,
                        &fallback,
                        &mut states,
                        mode,
                        &slot,
                    )
                })
        });

        process_progress.finish();
        result?;
    }

    println!(
        "Progress: 100.00% ({}/{})",
        format_with_commas(total_rows),
        format_with_commas(total_rows)
    );

    Ok(())
}



fn parallel_file_count(file_count: usize, requested_jobs: usize) -> usize {
    requested_jobs.max(1).min(file_count.max(1))
}

fn parallel_summary(parallel_files: usize) -> String {
    if parallel_files == 1 {
        "1 file".to_owned()
    } else {
        format!("{parallel_files} files")
    }
}

fn list_directory_and_confirm(
    dir: &Path,
    files: &[InputFile],
    mode: OutputMode,
    jobs: usize,
) -> Result<Vec<(InputFile, usize)>> {
    let parallel_files = parallel_file_count(files.len(), jobs);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallel_files)
        .build()
        .context("failed to build Rayon row-counting thread pool")?;

    println!("\nCounting rows with {}...", parallel_summary(parallel_files));
    let file_info = pool.install(|| {
        files
            .par_iter()
            .map(|file| {
                let row_count = count_rows(&file.path)
                    .with_context(|| format!("failed to count rows in {}", file.path.display()))?;
                Ok::<_, anyhow::Error>((file.clone(), row_count))
            })
            .collect::<Result<Vec<_>>>()
    })?;

    let total_files = file_info.len();
    let total_rows: usize = file_info.iter().map(|(_, row_count)| *row_count).sum();

    println!("\nDirectory: {}", dir.display());
    println!(
        "Total: {} files, {} rows",
        total_files,
        format_with_commas(total_rows)
    );
    println!("Files to process:");
    for (index, (file, row_count)) in file_info.iter().enumerate() {
        let file_name = file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?");
        println!(
            "  {}. {} ({} rows)",
            index + 1,
            file_name,
            format_with_commas(*row_count)
        );
    }
    println!(
        "Output mode: {}",
        match mode {
            OutputMode::Overwrite => "overwrite in place",
            OutputMode::Copy => "copy to .rsi.csv",
        }
    );

    if cfg!(test) || std::env::var("ADD_RSI_NO_CONFIRM").is_ok() {
        return Ok(file_info);
    }

    print!("\nProceed? [Y/N] ");
    io::stdout().flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    let answer = answer.trim().to_uppercase();

    if answer != "Y" {
        bail!("aborted by user");
    }

    Ok(file_info)
}

fn count_rows(path: &Path) -> Result<usize> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = Vec::with_capacity(64 * 1024);
    let mut rows = 0_usize;

    loop {
        buffer.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;

        if bytes_read == 0 {
            break;
        }

        rows += 1;
    }

    Ok(rows)
}


fn validate_windows(windows: &[usize]) -> Result<()> {
    if windows.is_empty() {
        bail!("at least one RSI window is required");
    }

    if windows.iter().any(|window| *window == 0) {
        bail!("RSI windows must be greater than zero");
    }

    Ok(())
}

fn collect_input_files(dir: &Path, prefix: &str, interval: &str) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();

    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };

        let Some((year, month)) = parse_input_filename(&file_name, prefix, interval) else {
            continue;
        };

        files.push(InputFile {
            path: entry.path(),
            year,
            month,
        });
    }

    files.sort_by(|left, right| {
        left.year
            .cmp(&right.year)
            .then(left.month.cmp(&right.month))
            .then(left.path.cmp(&right.path))
    });

    Ok(files)
}

fn parse_input_filename(file_name: &str, prefix: &str, interval: &str) -> Option<(u32, u32)> {
    let base_name = file_name.strip_suffix(".csv")?;
    let full_prefix = format!("{}-{}-", prefix, interval);
    let suffix = base_name.strip_prefix(&full_prefix)?;
    let (year_text, month_text) = suffix.split_once('-')?;

    let year = year_text.parse().ok()?;
    let month: u32 = month_text.parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }

    Some((year, month))
}

fn compute_start_states(
    file_info: &[(InputFile, usize)],
    windows: &[usize],
    fallback: &str,
    progress: &Arc<ProgressUi>,
) -> Result<Vec<Vec<RsiState>>> {
    let mut rolling_states = windows
        .iter()
        .copied()
        .map(RsiState::new)
        .collect::<Vec<_>>();
    let mut start_states = Vec::with_capacity(file_info.len());

    for (file, row_count) in file_info {
        let name = file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?");
        let slot = progress.acquire_slot(name, *row_count as u64);
        start_states.push(rolling_states.clone());
        scan_close_column_into_states(&file.path, &mut rolling_states, fallback, &slot)?;
    }

    Ok(start_states)
}

fn scan_close_column_into_states(
    input_path: &Path,
    states: &mut [RsiState],
    fallback: &str,
    progress: &ProgressSlot,
) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_path)
        .with_context(|| format!("failed to open {}", input_path.display()))?;

    let mut pending_progress = 0_u64;

    for result in reader.records() {
        let record = result.with_context(|| {
            format!("failed to read CSV record from {}", input_path.display())
        })?;
        let close = parse_close(&record);

        for state in states.iter_mut() {
            let _ = state.next_cell(close, fallback);
        }

        pending_progress += 1;
        if pending_progress >= 8192 {
            progress.inc(pending_progress);
            pending_progress = 0;
        }
    }

    if pending_progress > 0 {
        progress.inc(pending_progress);
    }

    Ok(())
}

fn process_file(
    input_path: &Path,
    preserved_columns: usize,
    fallback: &str,
    states: &mut [RsiState],
    mode: OutputMode,
    progress: &ProgressSlot,
) -> Result<()> {
    let output_path = build_output_path(input_path, mode)?;
    let temp_path = temp_output_path(&output_path)?;

    let write_result = (|| -> Result<()> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_path(input_path)
            .with_context(|| format!("failed to open {}", input_path.display()))?;

        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_path(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;

        let mut pending_progress = 0_u64;

        for result in reader.records() {
            let record = result.with_context(|| {
                format!("failed to read CSV record from {}", input_path.display())
            })?;
            let close = parse_close(&record);

            let mut output: Vec<String> =
                Vec::with_capacity(record.len().min(preserved_columns) + states.len());
            output.extend(record.iter().take(preserved_columns).map(str::to_owned));

            for state in states.iter_mut() {
                output.push(state.next_cell(close, fallback));
            }

            writer.write_record(output.iter().map(String::as_str)).with_context(|| {
                format!("failed to write CSV record to {}", temp_path.display())
            })?;

            pending_progress += 1;
            if pending_progress >= 8192 {
                progress.inc(pending_progress);
                pending_progress = 0;
            }
        }

        if pending_progress > 0 {
            progress.inc(pending_progress);
        }

        writer.flush()?;
        Ok(())
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }

    fs::rename(&temp_path, &output_path).with_context(|| {
        format!(
            "failed to move {} into {}",
            temp_path.display(),
            output_path.display()
        )
    })?;

    Ok(())
}


fn build_output_path(input_path: &Path, mode: OutputMode) -> Result<PathBuf> {
    match mode {
        OutputMode::Overwrite => Ok(input_path.to_path_buf()),
        OutputMode::Copy => {
            let stem = input_path
                .file_stem()
                .and_then(|value| value.to_str())
                .with_context(|| {
                    format!("failed to derive output name for {}", input_path.display())
                })?;
            Ok(input_path.with_file_name(format!("{stem}.rsi.csv")))
        }
    }
}

fn temp_output_path(output_path: &Path) -> Result<PathBuf> {
    let file_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .with_context(|| format!("invalid output path {}", output_path.display()))?;

    Ok(output_path.with_file_name(format!(".{file_name}.tmp-{}", unique_token())))
}

fn unique_token() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn format_with_commas(n: usize) -> String {
    let digits = n.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(character);
    }

    formatted
}


fn parse_close(record: &StringRecord) -> Option<f64> {
    record
        .get(CLOSE_COLUMN_INDEX)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
}

fn format_rsi(average_gain: f64, average_loss: f64, fallback: &str) -> String {
    if !average_gain.is_finite() || !average_loss.is_finite() {
        return fallback.to_owned();
    }

    if average_loss == 0.0 {
        return if average_gain == 0.0 {
            "50".to_owned()
        } else {
            "100".to_owned()
        };
    }

    let relative_strength = average_gain / average_loss;
    if !relative_strength.is_finite() {
        return fallback.to_owned();
    }

    let rsi = 100.0 - (100.0 / (1.0 + relative_strength));
    if !rsi.is_finite() {
        return fallback.to_owned();
    }

    rsi.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("add-rsi-test-{}", unique_token()));
            fs::create_dir(&path).expect("create test directory");
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn cli_parses_windows_fallback_and_copy_mode() {
        let cli = Cli::try_parse_from([
            "add-rsi",
            "-d",
            "/tmp/input",
            "-i",
            "1s",
            "-w",
            "5,10,20",
            "-f",
            "DIV0",
            "-C",
        ])
        .expect("parse CLI");

        assert_eq!(cli.dir, PathBuf::from("/tmp/input"));
        assert_eq!(cli.interval, "1s");
        assert_eq!(cli.windows, vec![5, 10, 20]);
        assert_eq!(cli.fallback, "DIV0");
        assert_eq!(cli.column, 12);
        assert!(cli.copy);
        assert!(!cli.overwrite);
    }

    #[test]
    fn parse_input_filename_accepts_expected_pattern() {
        assert_eq!(
            parse_input_filename("SOLUSDT-1s-2024-01.csv", "SOLUSDT", "1s"),
            Some((2024, 1))
        );
        assert_eq!(
            parse_input_filename("SOL-USDT-5m-2024-12.csv", "SOL-USDT", "5m"),
            Some((2024, 12))
        );
        assert_eq!(
            parse_input_filename("SOLUSDT-1s-2024-13.csv", "SOLUSDT", "1s"),
            None
        );
        assert_eq!(
            parse_input_filename("SOLUSDT-1s-2024-01.rsi.csv", "SOLUSDT", "1s"),
            None
        );
        assert_eq!(
            parse_input_filename("BTCUSDT-1s-2024-01.csv", "SOLUSDT", "1s"),
            None
        );
    }

    #[test]
    fn collect_input_files_sorts_and_ignores_subdirectories() {
        let test_dir = TestDir::new();
        fs::write(test_dir.path.join("SOLUSDT-1s-2024-02.csv"), "").expect("write CSV");
        fs::write(test_dir.path.join("SOLUSDT-1s-2023-12.csv"), "").expect("write CSV");
        fs::write(test_dir.path.join("BTCUSDT-1s-2024-01.csv"), "").expect("write CSV");

        let nested_dir = test_dir.path.join("nested");
        fs::create_dir(&nested_dir).expect("create nested directory");
        fs::write(nested_dir.join("SOLUSDT-1s-2022-01.csv"), "").expect("write nested CSV");

        let files = collect_input_files(&test_dir.path, "SOLUSDT", "1s").expect("collect files");
        let names = files
            .iter()
            .map(|file| {
                file.path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec!["SOLUSDT-1s-2023-12.csv", "SOLUSDT-1s-2024-02.csv"]
        );
    }

    #[test]
    fn overwrite_mode_truncates_columns_and_appends_rsi() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &["drop-a", "drop-b"]),
                kline_row("9", &["drop-c", "drop-d"]),
            ],
        );

        run(Cli {
            dir: test_dir.path.clone(),
            name: "SOLUSDT".to_owned(),
            interval: "1s".to_owned(),
            column: 12,
            windows: vec![1],
            fallback: "NA".to_owned(),
            no_carry: false,
            carry: true,
            jobs: 5,
            overwrite: true,
            copy: false,
        })
        .expect("run overwrite mode");

        let rows = read_rows(&file_path);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 13);
        assert_eq!(rows[0][12], "NA");
        assert_eq!(rows[1].len(), 13);
        assert_eq!(rows[1][11], "0");
        assert_eq!(rows[1][12], "0");
        assert!(!rows[1].contains(&"drop-c".to_owned()));
        assert!(!rows[1].contains(&"drop-d".to_owned()));
    }

    #[test]
    fn copy_mode_preserves_original_and_writes_rsi_sibling() {
        let test_dir = TestDir::new();
        let input_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &input_path,
            &[
                kline_row("10", &["keep-extra-a", "keep-extra-b"]),
                kline_row("9", &["keep-extra-c", "keep-extra-d"]),
            ],
        );

        run(Cli {
            dir: test_dir.path.clone(),
            name: "SOLUSDT".to_owned(),
            interval: "1s".to_owned(),
            column: 12,
            windows: vec![1],
            fallback: "NA".to_owned(),
            no_carry: false,
            carry: true,
            jobs: 5,
            overwrite: false,
            copy: true,
        })
        .expect("run copy mode");

        let original_rows = read_rows(&input_path);
        let copy_path = test_dir.path.join("SOLUSDT-1s-2024-01.rsi.csv");
        let copy_rows = read_rows(&copy_path);

        assert_eq!(original_rows[0].len(), 14);
        assert!(original_rows[0].contains(&"keep-extra-a".to_owned()));
        assert_eq!(copy_rows[0].len(), 13);
        assert_eq!(copy_rows[0][12], "NA");
    }

    #[test]
    fn carries_rsi_state_across_month_boundaries() {
        let test_dir = TestDir::new();
        let january = test_dir.path.join("SOLUSDT-1s-2024-01.csv");
        let february = test_dir.path.join("SOLUSDT-1s-2024-02.csv");

        write_rows(&january, &[kline_row("10", &[]), kline_row("11", &[])]);
        write_rows(&february, &[kline_row("10", &[]), kline_row("12", &[])]);

        run(Cli {
            dir: test_dir.path.clone(),
            name: "SOLUSDT".to_owned(),
            interval: "1s".to_owned(),
            column: 12,
            windows: vec![2],
            fallback: "NA".to_owned(),
            no_carry: false,
            carry: true,
            jobs: 5,
            overwrite: true,
            copy: false,
        })
        .expect("run across files");

        let february_rows = read_rows(&february);
        assert_eq!(february_rows[0][12].parse::<f64>().unwrap(), 50.0);

        let second_value = february_rows[1][12].parse::<f64>().unwrap();
        assert!((second_value - 83.33333333333333).abs() < 1e-12);
    }

    #[test]
    fn rsi_returns_100_when_there_are_only_gains() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &[]),
                kline_row("11", &[]),
                kline_row("12", &[]),
            ],
        );

        run(Cli {
            dir: test_dir.path.clone(),
            name: "SOLUSDT".to_owned(),
            interval: "1s".to_owned(),
            column: 12,
            windows: vec![2],
            fallback: "DIV0".to_owned(),
            no_carry: false,
            carry: true,
            jobs: 5,
            overwrite: true,
            copy: false,
        })
        .expect("run with fallback");

        let rows = read_rows(&file_path);
        assert_eq!(rows[0][12], "DIV0");
        assert_eq!(rows[1][12], "DIV0");
        assert_eq!(rows[2][12], "100");
    }

    fn kline_row(close: &str, trailing: &[&str]) -> Vec<String> {
        let mut row = vec![
            "0".to_owned(),
            "1.0".to_owned(),
            "2.0".to_owned(),
            "0.5".to_owned(),
            close.to_owned(),
            "100.0".to_owned(),
            "1".to_owned(),
            "200.0".to_owned(),
            "5".to_owned(),
            "50.0".to_owned(),
            "100.0".to_owned(),
            "0".to_owned(),
        ];
        row.extend(trailing.iter().map(|value| (*value).to_owned()));
        row
    }

    fn write_rows(path: &Path, rows: &[Vec<String>]) {
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_path(path)
            .expect("create CSV writer");

        for row in rows {
            writer.write_record(row).expect("write CSV row");
        }

        writer.flush().expect("flush CSV writer");
    }

    fn read_rows(path: &Path) -> Vec<Vec<String>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .expect("open CSV reader");

        reader
            .records()
            .map(|record| {
                record
                    .expect("read CSV row")
                    .iter()
                    .map(|field| field.to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

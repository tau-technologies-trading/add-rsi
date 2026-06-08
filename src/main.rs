use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;

const CLOSE_COLUMN_INDEX: usize = 4;
const DEFAULT_DATA_DIR: &str = "../data/BTCUSDT/";
const DEFAULT_ALL_DATA_DIR: &str = "../data/";
static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Parser, Debug)]
#[command(
    about = "Add Wilder RSI columns to Binance Vision CSV files",
    long_about = "Add Wilder RSI columns to Binance Vision CSV files.\n\nBy default this processes every matching file independently in parallel, overwriting the original CSVs after confirmation. Use --auto to skip files whose inspected row already has the requested RSI columns filled in and skip the confirmation prompt. In --auto without --carry, each file is classified from its last CSV row. In --auto with --carry, each file is classified from its first CSV row, and skipped files are not row-counted, scanned, or written. Use --carry for RSI continuity across the selected files. Use --copy if you want sibling .rsi.csv outputs instead of replacing the originals.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  add-rsi\n  add-rsi --jobs 3\n  add-rsi --auto --carry --jobs 5\n  add-rsi --all --copy --window 14,64,256\n\nNotes:\n  Without --all, --dir defaults to ../data/BTCUSDT/.\n  With --all, --dir defaults to ../data/ unless -d/--dir is provided.\n  --jobs is a maximum; the program uses fewer workers if fewer files or carry groups exist.\n  --auto without --carry skips files whose last CSV row has all requested RSI columns present and non-empty.\n  --auto --carry skips files whose first CSV row has all requested RSI columns present and non-empty. Skipped files are not written, row-counted, or scanned.\n  --all recursively processes matching CSV files, using each CSV parent directory name as its file-name prefix and carrying RSI state independently per parent directory. Extra columns are dropped before RSI values are appended.\n  Set ADD_RSI_NO_CONFIRM=1 to skip the confirmation prompt."
)]
struct Cli {
    /// Directory containing the CSV files to process.
    #[arg(short = 'd', long = "dir", default_value = DEFAULT_DATA_DIR)]
    dir: PathBuf,

    /// File name prefix (e.g. SOLUSDT).
    #[arg(short = 'n', long = "name", default_value = "BTCUSDT")]
    name: String,

    /// Time interval in seconds (e.g. 1s, 5m, 1h).
    #[arg(short = 'i', long = "interval", default_value = "1s")]
    interval: String,

    /// Number of leading columns to preserve before appending RSI values.
    #[arg(short = 'c', long = "column", alias = "columns", default_value_t = 12)]
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

    /// Maximum number of files to process in parallel during inspection and output processing.
    #[arg(short = 'j', long = "jobs", default_value_t = 5)]
    jobs: usize,

    /// Automatically skip files that already appear to have appended columns and skip confirmation.
    #[arg(short = 'a', long = "auto", conflicts_with = "all")]
    auto: bool,

    /// Process every matching file, including files with more than --column columns.
    #[arg(long = "all", conflicts_with = "auto")]
    all: bool,

    /// Explicitly overwrite the original files in place.
    #[arg(short = 'o', long = "overwrite", conflicts_with = "copy")]
    overwrite: bool,

    /// Write processed sibling files with a .rsi.csv suffix.
    #[arg(short = 'C', long = "copy", conflicts_with = "overwrite")]
    copy: bool,

    /// Print version information and exit.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::SetTrue, required = false)]
    version_flag: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputMode {
    Overwrite,
    Copy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectionMode {
    Auto,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutoInspectMode {
    FirstRow,
    LastRow,
}

#[derive(Clone, Copy, Debug)]
struct InspectionOptions {
    mode: OutputMode,
    selection_mode: SelectionMode,
    auto_inspect_mode: AutoInspectMode,
    expected_columns: usize,
    rsi_column_count: usize,
    jobs: usize,
    skip_confirmation: bool,
}

#[derive(Clone, Debug)]
struct InputFile {
    path: PathBuf,
    group: PathBuf,
    year: u32,
    month: u32,
}

#[derive(Clone, Debug)]
struct FileInfo {
    file: InputFile,
    row_count: Option<usize>,
    min_columns: Option<usize>,
    max_columns: Option<usize>,
    auto_processable: Option<bool>,
}

impl FileInfo {
    fn is_auto_processable(&self, expected_columns: usize) -> bool {
        self.auto_processable.unwrap_or_else(|| {
            self.max_columns
                .map(|max_columns| max_columns <= expected_columns)
                .unwrap_or(true)
        })
    }

    fn has_extra_columns(&self, expected_columns: usize) -> bool {
        self.max_columns
            .map(|max_columns| max_columns > expected_columns)
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileStats {
    row_count: Option<usize>,
    min_columns: Option<usize>,
    max_columns: Option<usize>,
    auto_processable: Option<bool>,
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
}

struct ProgressSlot {
    file_bar: ProgressBar,
    total_bar: ProgressBar,
    finished: bool,
}

impl ProgressUi {
    fn new(
        stage: impl Into<String>,
        total_rows: Option<u64>,
        parallel_files: usize,
        unit_label: &str,
    ) -> Arc<Self> {
        let multi = MultiProgress::new();

        let total_bar = match total_rows {
            Some(total_rows) => multi.add(ProgressBar::new(total_rows)),
            None => multi.add(ProgressBar::new_spinner()),
        };
        total_bar.set_prefix(stage.into());
        total_bar.set_message(summary(parallel_files, unit_label));
        total_bar.set_style(total_progress_style(total_rows.is_some()));

        Arc::new(Self { multi, total_bar })
    }

    fn acquire_slot(self: &Arc<Self>, name: &str, total_rows: Option<u64>) -> ProgressSlot {
        let file_bar = match total_rows {
            Some(total_rows) => self.multi.add(ProgressBar::new(total_rows)),
            None => self.multi.add(ProgressBar::new_spinner()),
        };
        file_bar.set_style(file_progress_style(total_rows.is_some()));
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

fn total_progress_style(has_total: bool) -> ProgressStyle {
    if has_total {
        return ProgressStyle::with_template(
            "{prefix:<7} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:>3}% {pos}/{len} rows ({per_sec}, ETA {eta}) {msg}",
        )
        .expect("valid total progress-bar template")
        .progress_chars("##-");
    }

    ProgressStyle::with_template(
        "{prefix:<7} [{elapsed_precise}] {spinner:.green} {pos} rows ({per_sec}) {msg}",
    )
    .expect("valid total spinner template")
}

fn file_progress_style(has_total: bool) -> ProgressStyle {
    if has_total {
        return ProgressStyle::with_template(
            "  {msg:<34} [{bar:28.green/black}] {percent:>3}% {pos}/{len}",
        )
        .expect("valid file progress-bar template")
        .progress_chars("##-");
    }

    ProgressStyle::with_template("  {msg:<34} {spinner:.green} {pos} rows")
        .expect("valid file spinner template")
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
    if std::env::args_os()
        .skip(1)
        .any(|arg| arg == "-v" || arg == "--version")
    {
        print_version();
        return;
    }

    let dir_was_provided = dir_arg_was_provided();

    if let Err(error) = run_with_dir_source(Cli::parse(), dir_was_provided) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn dir_arg_was_provided() -> bool {
    std::env::args_os().skip(1).any(|arg| {
        arg == "-d"
            || arg == "--dir"
            || arg
                .to_str()
                .map(|arg| arg.starts_with("--dir="))
                .unwrap_or(false)
    })
}

fn print_version() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
}

#[cfg(test)]
fn run(cli: Cli) -> Result<()> {
    run_with_dir_source(cli, true)
}

fn run_with_dir_source(mut cli: Cli, dir_was_provided: bool) -> Result<()> {
    let _ = cli.version_flag;
    let _ = cli.no_carry;

    if cli.all && !dir_was_provided && cli.dir == Path::new(DEFAULT_DATA_DIR) {
        cli.dir = PathBuf::from(DEFAULT_ALL_DATA_DIR);
    }

    if !cli.dir.is_dir() {
        bail!("{} is not a directory", cli.dir.display());
    }

    validate_windows(&cli.windows)?;

    let files = if cli.all {
        collect_all_input_files(&cli.dir, &cli.interval)?
    } else {
        collect_input_files(&cli.dir, &cli.name, &cli.interval)?
    };
    if files.is_empty() {
        if cli.all {
            bail!(
                "no matching CSV files found recursively in {} using parent-directory prefixes and interval {}",
                cli.dir.display(),
                cli.interval
            );
        } else {
            bail!(
                "no matching CSV files found in {} for prefix {}-{}",
                cli.dir.display(),
                cli.name,
                cli.interval
            );
        }
    }

    let mode = match (cli.overwrite, cli.copy) {
        (_, true) => OutputMode::Copy,
        _ => OutputMode::Overwrite,
    };
    let selection_mode = if cli.auto {
        SelectionMode::Auto
    } else {
        SelectionMode::All
    };
    let skip_confirmation = cli.auto || std::env::var("ADD_RSI_NO_CONFIRM").is_ok();
    let auto_inspect_mode = if cli.carry {
        AutoInspectMode::FirstRow
    } else {
        AutoInspectMode::LastRow
    };

    let file_info = list_directory_and_confirm(
        &cli.dir,
        &files,
        InspectionOptions {
            mode,
            selection_mode,
            auto_inspect_mode,
            expected_columns: cli.column,
            rsi_column_count: cli.windows.len(),
            jobs: cli.jobs,
            skip_confirmation,
        },
    )?;
    let files_to_process = select_process_files(&file_info, selection_mode, cli.column);

    if files_to_process.is_empty() {
        bail!(
            "no files eligible for processing with --auto and --column {}; use --all to process every matching file",
            cli.column
        );
    }

    let process_total_rows = sum_counted_rows(&files_to_process);

    if cli.carry {
        let file_groups = group_file_info(files_to_process);
        let parallel_groups = parallel_file_count(file_groups.len(), cli.jobs);
        println!(
            "\nCarryover mode: processing {} in parallel; RSI state resets for each parent-directory group.",
            summary(parallel_groups, "group")
        );
        if selection_mode == SelectionMode::Auto {
            println!("Carryover mode: skipped --auto files are not scanned.");
        }

        let process_progress = ProgressUi::new(
            "TOTAL ",
            process_total_rows.map(|row_count| row_count as u64),
            parallel_groups,
            "folder",
        );
        let windows = cli.windows.clone();
        let fallback = cli.fallback.clone();
        let column = cli.column;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_groups)
            .build()
            .context("failed to build Rayon thread pool")?;

        let result = pool.install(|| {
            file_groups.into_par_iter().try_for_each(|group| {
                let mut states = windows
                    .iter()
                    .copied()
                    .map(RsiState::new)
                    .collect::<Vec<_>>();

                for file_info in group {
                    let name = progress_name(&file_info.file);
                    let slot = process_progress
                        .acquire_slot(&name, file_info.row_count.map(|row_count| row_count as u64));
                    process_file(
                        &file_info.file.path,
                        column,
                        &fallback,
                        &mut states,
                        mode,
                        &slot,
                    )?;
                }

                Ok::<_, anyhow::Error>(())
            })
        });

        process_progress.finish();
        result?;
    } else {
        let parallel_files = parallel_file_count(files_to_process.len(), cli.jobs);
        println!(
            "\nNo-carry mode: processing {} in parallel.",
            parallel_summary(parallel_files)
        );

        let process_progress = ProgressUi::new(
            "TOTAL ",
            process_total_rows.map(|row_count| row_count as u64),
            parallel_files,
            "file",
        );
        let windows = cli.windows.clone();
        let fallback = cli.fallback.clone();
        let column = cli.column;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_files)
            .build()
            .context("failed to build Rayon thread pool")?;

        let result = pool.install(|| {
            files_to_process.into_par_iter().try_for_each(|file_info| {
                let name = progress_name(&file_info.file);
                let slot = process_progress
                    .acquire_slot(&name, file_info.row_count.map(|row_count| row_count as u64));
                let mut states = windows
                    .iter()
                    .copied()
                    .map(RsiState::new)
                    .collect::<Vec<_>>();

                process_file(
                    &file_info.file.path,
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

    match process_total_rows {
        Some(process_total_rows) => println!(
            "Progress: 100.00% ({}/{})",
            format_with_commas(process_total_rows),
            format_with_commas(process_total_rows)
        ),
        None => println!("Progress: completed (rows not counted)."),
    }

    Ok(())
}

fn parallel_file_count(file_count: usize, requested_jobs: usize) -> usize {
    requested_jobs.max(1).min(file_count.max(1))
}

fn summary(count: usize, unit: &str) -> String {
    if count == 1 {
        format!("1 {unit}")
    } else {
        format!("{count} {unit}s")
    }
}

fn parallel_summary(parallel_files: usize) -> String {
    summary(parallel_files, "file")
}

fn group_file_info(file_info: Vec<FileInfo>) -> Vec<Vec<FileInfo>> {
    let mut groups = Vec::<Vec<FileInfo>>::new();

    for file_info in file_info {
        if let Some(group) = groups
            .last_mut()
            .filter(|group| group[0].file.group == file_info.file.group)
        {
            group.push(file_info);
        } else {
            groups.push(vec![file_info]);
        }
    }

    groups
}

fn progress_name(file: &InputFile) -> String {
    let group_name = file
        .group
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("?");
    let file_name = file
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("?");

    format!("{group_name}/{file_name}")
}

fn list_directory_and_confirm(
    dir: &Path,
    files: &[InputFile],
    options: InspectionOptions,
) -> Result<Vec<FileInfo>> {
    let parallel_files = parallel_file_count(files.len(), options.jobs);
    let file_info = match options.selection_mode {
        SelectionMode::Auto => {
            println!(
                "\n{} with up to {} in parallel...",
                match options.auto_inspect_mode {
                    AutoInspectMode::FirstRow => "Checking first-row RSI cells",
                    AutoInspectMode::LastRow => "Checking last-row RSI cells",
                },
                parallel_summary(parallel_files)
            );
            inspect_files_parallel(files, options, parallel_files)?
        }
        SelectionMode::All => inspect_all_files_by_folder(files, options)?,
    };

    let total_files = file_info.len();
    let total_rows = sum_counted_rows(&file_info);
    let process_files =
        select_process_files(&file_info, options.selection_mode, options.expected_columns);
    let process_rows = sum_counted_rows(&process_files);
    let skipped_files = file_info
        .iter()
        .filter(|file_info| {
            !should_process_file(file_info, options.selection_mode, options.expected_columns)
        })
        .cloned()
        .collect::<Vec<_>>();

    println!("\nDirectory: {}", dir.display());
    println!(
        "Matching files: {} files, {}",
        total_files,
        describe_total_rows(total_rows)
    );
    println!("Column target: {}", options.expected_columns);
    println!(
        "Selection mode: {}",
        match options.selection_mode {
            SelectionMode::Auto => match options.auto_inspect_mode {
                AutoInspectMode::FirstRow => {
                    "auto: skip files whose first row has all requested RSI columns filled"
                }
                AutoInspectMode::LastRow => {
                    "auto: skip files whose last row has all requested RSI columns filled"
                }
            },
            SelectionMode::All => "all: process every matching file",
        }
    );
    println!(
        "Files to process: {} files, {}",
        process_files.len(),
        describe_total_rows(process_rows)
    );

    if options.selection_mode == SelectionMode::All {
        print_folder_summary("Folders to process:", &process_files);
    }

    print_file_list("Files to process:", &process_files);

    if !skipped_files.is_empty() {
        print_file_list("Files skipped by --auto:", &skipped_files);
        if options.selection_mode == SelectionMode::Auto {
            match options.auto_inspect_mode {
                AutoInspectMode::FirstRow => println!(
                    "Skipped files are not written, row-counted, or scanned. RSI state carries across selected files only."
                ),
                AutoInspectMode::LastRow => {
                    println!("Skipped files are not written or processed.")
                }
            }
        }
    }

    warn_about_extra_columns(
        &file_info,
        options.selection_mode,
        options.auto_inspect_mode,
        options.expected_columns,
    );
    warn_about_missing_months(&file_info);

    println!(
        "Output mode: {}",
        match options.mode {
            OutputMode::Overwrite => "overwrite in place",
            OutputMode::Copy => "copy to .rsi.csv",
        }
    );

    if cfg!(test) || options.skip_confirmation {
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

fn inspect_files_parallel(
    files: &[InputFile],
    options: InspectionOptions,
    parallel_files: usize,
) -> Result<Vec<FileInfo>> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallel_files)
        .build()
        .context("failed to build Rayon inspection thread pool")?;

    pool.install(|| {
        files
            .par_iter()
            .map(|file| {
                let stats = analyze_file(
                    &file.path,
                    options.selection_mode,
                    options.auto_inspect_mode,
                    options.expected_columns,
                    options.rsi_column_count,
                )
                .with_context(|| format!("failed to inspect {}", file.path.display()))?;
                Ok::<_, anyhow::Error>(FileInfo {
                    file: file.clone(),
                    row_count: stats.row_count,
                    min_columns: stats.min_columns,
                    max_columns: stats.max_columns,
                    auto_processable: stats.auto_processable,
                })
            })
            .collect::<Result<Vec<_>>>()
    })
}

fn inspect_all_files_by_folder(
    files: &[InputFile],
    options: InspectionOptions,
) -> Result<Vec<FileInfo>> {
    let groups = group_input_files_by_folder(files);
    let parallel_files = groups
        .iter()
        .map(|group| parallel_file_count(group.len(), options.jobs))
        .max()
        .unwrap_or_else(|| parallel_file_count(files.len(), options.jobs));

    println!(
        "\nCounting rows and columns folder by folder, with up to {} in parallel per folder...",
        parallel_summary(parallel_files)
    );
    print_input_folder_summary("Folders to scan:", &groups);

    let mut file_info = Vec::with_capacity(files.len());

    for group in groups {
        let folder = group[0].group.display();
        let folder_parallel_files = parallel_file_count(group.len(), options.jobs);
        println!(
            "\nCounting {folder}: {} files with up to {} in parallel...",
            group.len(),
            parallel_summary(folder_parallel_files)
        );

        let mut group_info = inspect_files_parallel(group, options, folder_parallel_files)?;
        println!(
            "Counted {folder}: {} files, {}",
            group_info.len(),
            describe_total_rows(sum_counted_rows(&group_info))
        );
        file_info.append(&mut group_info);
    }

    Ok(file_info)
}

fn group_input_files_by_folder(files: &[InputFile]) -> Vec<&[InputFile]> {
    let mut groups = Vec::new();
    let mut start = 0_usize;

    while start < files.len() {
        let mut end = start + 1;
        while end < files.len() && files[end].group == files[start].group {
            end += 1;
        }

        groups.push(&files[start..end]);
        start = end;
    }

    groups
}

fn sum_counted_rows(file_info: &[FileInfo]) -> Option<usize> {
    let mut total = 0_usize;

    for file_info in file_info {
        total += file_info.row_count?;
    }

    Some(total)
}

fn describe_total_rows(row_count: Option<usize>) -> String {
    row_count
        .map(|row_count| format!("{} counted rows", format_with_commas(row_count)))
        .unwrap_or_else(|| "rows not counted".to_owned())
}

fn select_process_files(
    file_info: &[FileInfo],
    selection_mode: SelectionMode,
    expected_columns: usize,
) -> Vec<FileInfo> {
    file_info
        .iter()
        .filter(|file_info| should_process_file(file_info, selection_mode, expected_columns))
        .cloned()
        .collect()
}

fn should_process_file(
    file_info: &FileInfo,
    selection_mode: SelectionMode,
    expected_columns: usize,
) -> bool {
    match selection_mode {
        SelectionMode::Auto => file_info.is_auto_processable(expected_columns),
        SelectionMode::All => true,
    }
}

fn print_file_list(title: &str, file_info: &[FileInfo]) {
    if file_info.is_empty() {
        println!("{title} none");
        return;
    }

    println!("{title}");
    for (index, file_info) in file_info.iter().enumerate() {
        let file_name = progress_name(&file_info.file);
        println!(
            "  {}. {} ({} rows, {})",
            index + 1,
            file_name,
            describe_row_count(file_info.row_count),
            describe_column_range(file_info.min_columns, file_info.max_columns)
        );
    }
}

fn print_input_folder_summary(title: &str, groups: &[&[InputFile]]) {
    if groups.is_empty() {
        println!("{title} none");
        return;
    }

    println!("\n{title}");
    for group in groups {
        println!(
            "  {}: {} file{}",
            group[0].group.display(),
            group.len(),
            if group.len() == 1 { "" } else { "s" }
        );
    }
}

fn print_folder_summary(title: &str, file_info: &[FileInfo]) {
    if file_info.is_empty() {
        println!("{title} none");
        return;
    }

    println!("{title}");
    for group in group_file_info(file_info.to_vec()) {
        println!(
            "  {}: {} file{}, {}",
            group[0].file.group.display(),
            group.len(),
            if group.len() == 1 { "" } else { "s" },
            describe_total_rows(sum_counted_rows(&group))
        );
    }
}

fn describe_row_count(row_count: Option<usize>) -> String {
    row_count
        .map(format_with_commas)
        .unwrap_or_else(|| "not counted".to_owned())
}

fn describe_column_range(min_columns: Option<usize>, max_columns: Option<usize>) -> String {
    match (min_columns, max_columns) {
        (Some(min_columns), Some(max_columns)) if min_columns == max_columns => {
            format!("{min_columns} columns")
        }
        (Some(min_columns), Some(max_columns)) => format!("{min_columns}-{max_columns} columns"),
        _ => "no CSV rows".to_owned(),
    }
}

fn warn_about_extra_columns(
    file_info: &[FileInfo],
    selection_mode: SelectionMode,
    auto_inspect_mode: AutoInspectMode,
    expected_columns: usize,
) {
    match selection_mode {
        SelectionMode::Auto => {
            let skipped_count = file_info
                .iter()
                .filter(|file_info| !file_info.is_auto_processable(expected_columns))
                .count();
            let selected_extra_count = file_info
                .iter()
                .filter(|file_info| file_info.is_auto_processable(expected_columns))
                .filter(|file_info| file_info.has_extra_columns(expected_columns))
                .count();

            if skipped_count > 0 {
                println!(
                    "\nAuto mode: skipped {skipped_count} file{} whose {} already has all requested RSI columns filled.",
                    if skipped_count == 1 { "" } else { "s" },
                    auto_inspected_row_name(auto_inspect_mode)
                );
            }

            if selected_extra_count > 0 {
                println!(
                    "\nWARNING: processing {selected_extra_count} selected file{} with rows over {expected_columns} columns; extra columns will be dropped before RSI values are appended.",
                    if selected_extra_count == 1 { "" } else { "s" }
                );
            }
        }
        SelectionMode::All => {
            let extra_count = file_info
                .iter()
                .filter(|file_info| file_info.has_extra_columns(expected_columns))
                .count();

            if extra_count == 0 {
                return;
            }

            println!(
                "\nWARNING: processing {extra_count} file{} with more than {expected_columns} columns; extra columns will be dropped before RSI values are appended.",
                if extra_count == 1 { "" } else { "s" }
            );
        }
    }
}

fn auto_inspected_row_name(auto_inspect_mode: AutoInspectMode) -> &'static str {
    match auto_inspect_mode {
        AutoInspectMode::FirstRow => "first row",
        AutoInspectMode::LastRow => "last row",
    }
}

fn analyze_file(
    path: &Path,
    selection_mode: SelectionMode,
    auto_inspect_mode: AutoInspectMode,
    expected_columns: usize,
    rsi_column_count: usize,
) -> Result<FileStats> {
    match selection_mode {
        SelectionMode::Auto => match auto_inspect_mode {
            AutoInspectMode::FirstRow => {
                analyze_file_for_auto_first_row(path, expected_columns, rsi_column_count)
            }
            AutoInspectMode::LastRow => {
                analyze_file_for_auto_last_row(path, expected_columns, rsi_column_count)
            }
        },
        SelectionMode::All => analyze_file_fully(path),
    }
}

fn analyze_file_fully(path: &Path) -> Result<FileStats> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let mut row_count = 0_usize;
    let mut min_columns = None::<usize>;
    let mut max_columns = None::<usize>;

    for result in reader.records() {
        let record =
            result.with_context(|| format!("failed to read CSV record from {}", path.display()))?;
        let columns = record.len();
        row_count += 1;
        min_columns = Some(min_columns.map_or(columns, |current| current.min(columns)));
        max_columns = Some(max_columns.map_or(columns, |current| current.max(columns)));
    }

    Ok(FileStats {
        row_count: Some(row_count),
        min_columns,
        max_columns,
        auto_processable: None,
    })
}

fn analyze_file_for_auto_first_row(
    path: &Path,
    expected_columns: usize,
    rsi_column_count: usize,
) -> Result<FileStats> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let Some(result) = reader.records().next() else {
        return Ok(FileStats {
            row_count: Some(0),
            min_columns: None,
            max_columns: None,
            auto_processable: Some(true),
        });
    };

    let record = result
        .with_context(|| format!("failed to read first CSV record from {}", path.display()))?;
    let columns = record.len();

    if row_has_completed_appended_rsi_columns(&record, expected_columns, rsi_column_count) {
        return Ok(FileStats {
            row_count: None,
            min_columns: Some(columns),
            max_columns: Some(columns),
            auto_processable: Some(false),
        });
    }

    let mut row_count = 1_usize;
    let mut min_columns = Some(columns);
    let mut max_columns = Some(columns);

    for result in reader.records() {
        let record =
            result.with_context(|| format!("failed to read CSV record from {}", path.display()))?;
        let columns = record.len();
        row_count += 1;
        min_columns = Some(min_columns.map_or(columns, |current| current.min(columns)));
        max_columns = Some(max_columns.map_or(columns, |current| current.max(columns)));
    }

    Ok(FileStats {
        row_count: Some(row_count),
        min_columns,
        max_columns,
        auto_processable: Some(true),
    })
}

fn analyze_file_for_auto_last_row(
    path: &Path,
    expected_columns: usize,
    rsi_column_count: usize,
) -> Result<FileStats> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let mut row_count = 0_usize;
    let mut min_columns = None::<usize>;
    let mut max_columns = None::<usize>;
    let mut last_record = None::<StringRecord>;

    for result in reader.records() {
        let record =
            result.with_context(|| format!("failed to read CSV record from {}", path.display()))?;
        let columns = record.len();
        row_count += 1;
        min_columns = Some(min_columns.map_or(columns, |current| current.min(columns)));
        max_columns = Some(max_columns.map_or(columns, |current| current.max(columns)));
        last_record = Some(record);
    }

    let auto_processable = last_record
        .as_ref()
        .map(|record| {
            !row_has_completed_appended_rsi_columns(record, expected_columns, rsi_column_count)
        })
        .unwrap_or(true);

    Ok(FileStats {
        row_count: Some(row_count),
        min_columns,
        max_columns,
        auto_processable: Some(auto_processable),
    })
}

fn row_has_completed_appended_rsi_columns(
    record: &StringRecord,
    expected_columns: usize,
    rsi_column_count: usize,
) -> bool {
    (0..rsi_column_count).all(|offset| {
        record
            .get(expected_columns + offset)
            .map(|field| !field.trim().is_empty())
            .unwrap_or(false)
    })
}

fn warn_about_missing_months(file_info: &[FileInfo]) {
    for group in group_file_info(file_info.to_vec()) {
        let missing_months = find_missing_months(&group);
        if missing_months.is_empty() {
            continue;
        }

        let group_name = group[0].file.group.display();
        println!(
            "\nWARNING: {} is missing {} month{} between first and last file: {}",
            group_name,
            missing_months.len(),
            if missing_months.len() == 1 { "" } else { "s" },
            missing_months.join(", ")
        );
        println!(
            "         Carryover will continue across the gap, but the missing month data is not being processed."
        );
    }
}

fn find_missing_months(file_info: &[FileInfo]) -> Vec<String> {
    let Some(first_file) = file_info.first() else {
        return Vec::new();
    };
    let Some(last_file) = file_info.last() else {
        return Vec::new();
    };

    let present_months = file_info
        .iter()
        .map(|file_info| month_index(file_info.file.year, file_info.file.month))
        .collect::<std::collections::HashSet<_>>();

    let start = month_index(first_file.file.year, first_file.file.month);
    let end = month_index(last_file.file.year, last_file.file.month);
    let mut missing = Vec::new();

    for month in start..=end {
        if !present_months.contains(&month) {
            missing.push(format_month_index(month));
        }
    }

    missing
}

fn month_index(year: u32, month: u32) -> u32 {
    (year * 12) + (month - 1)
}

fn format_month_index(month_index: u32) -> String {
    let year = month_index / 12;
    let month = (month_index % 12) + 1;
    format!("{year}-{month:02}")
}

fn validate_windows(windows: &[usize]) -> Result<()> {
    if windows.is_empty() {
        bail!("at least one RSI window is required");
    }

    if windows.contains(&0) {
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
            group: dir.to_path_buf(),
            year,
            month,
        });
    }

    sort_input_files(&mut files);

    Ok(files)
}

fn collect_all_input_files(dir: &Path, interval: &str) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();
    collect_all_input_files_into(dir, interval, &mut files)?;
    sort_input_files(&mut files);
    Ok(files)
}

fn collect_all_input_files_into(
    dir: &Path,
    interval: &str,
    files: &mut Vec<InputFile>,
) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();

        if file_type.is_dir() {
            collect_all_input_files_into(&path, interval, files)?;
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        let Some(parent) = path.parent() else {
            continue;
        };
        let group = parent.to_path_buf();
        let Some(prefix) = parent.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((year, month)) = parse_input_filename(file_name, prefix, interval) else {
            continue;
        };

        files.push(InputFile {
            path,
            group,
            year,
            month,
        });
    }

    Ok(())
}

fn sort_input_files(files: &mut [InputFile]) {
    files.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then(left.year.cmp(&right.year))
            .then(left.month.cmp(&right.month))
            .then(left.path.cmp(&right.path))
    });
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
            .flexible(true)
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

            writer
                .write_record(output.iter().map(String::as_str))
                .with_context(|| {
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
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
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
        assert!(!cli.auto);
        assert!(!cli.all);
    }

    #[test]
    fn cli_uses_btcusdt_defaults() {
        let cli = Cli::try_parse_from(["add-rsi"]).expect("parse default CLI");

        assert_eq!(cli.dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(cli.name, "BTCUSDT");
    }

    #[test]
    fn all_without_explicit_dir_uses_all_data_default() {
        let mut cli = Cli::try_parse_from(["add-rsi", "--all"]).expect("parse all CLI");

        if cli.all && cli.dir == Path::new(DEFAULT_DATA_DIR) {
            cli.dir = PathBuf::from(DEFAULT_ALL_DATA_DIR);
        }

        assert_eq!(cli.dir, PathBuf::from(DEFAULT_ALL_DATA_DIR));
    }

    #[test]
    fn cli_parses_auto_and_rejects_auto_all_together() {
        let cli =
            Cli::try_parse_from(["add-rsi", "-d", "/tmp/input", "--auto"]).expect("parse auto CLI");

        assert!(cli.auto);
        assert!(!cli.all);

        assert!(Cli::try_parse_from(["add-rsi", "-d", "/tmp/input", "--auto", "--all",]).is_err());
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
    fn collect_all_input_files_recurses_and_uses_parent_dir_prefix() {
        let test_dir = TestDir::new();
        let btc_dir = test_dir.path.join("spot").join("BTCUSDT");
        let eth_dir = test_dir.path.join("futures").join("linear").join("ETHUSDT");
        fs::create_dir_all(&btc_dir).expect("create BTC directory");
        fs::create_dir_all(&eth_dir).expect("create ETH directory");

        fs::write(btc_dir.join("BTCUSDT-1s-2024-02.csv"), "").expect("write BTC CSV");
        fs::write(btc_dir.join("ETHUSDT-1s-2024-01.csv"), "").expect("write wrong-prefix CSV");
        fs::write(btc_dir.join("BTCUSDT-1s-2024-03.rsi.csv"), "").expect("write RSI CSV");
        fs::write(eth_dir.join("ETHUSDT-1s-2024-01.csv"), "").expect("write ETH CSV");
        fs::write(eth_dir.join("ETHUSDT-5m-2024-01.csv"), "").expect("write wrong interval CSV");

        let files = collect_all_input_files(&test_dir.path, "1s").expect("collect all files");
        let names = files
            .iter()
            .map(|file| {
                file.path
                    .strip_prefix(&test_dir.path)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "futures/linear/ETHUSDT/ETHUSDT-1s-2024-01.csv",
                "spot/BTCUSDT/BTCUSDT-1s-2024-02.csv",
            ]
        );
    }

    #[test]
    fn group_input_files_by_folder_preserves_sorted_folder_runs() {
        let test_dir = TestDir::new();
        let btc_dir = test_dir.path.join("BTCUSDT");
        let eth_dir = test_dir.path.join("ETHUSDT");
        fs::create_dir_all(&btc_dir).expect("create BTC directory");
        fs::create_dir_all(&eth_dir).expect("create ETH directory");

        fs::write(btc_dir.join("BTCUSDT-1s-2024-01.csv"), "").expect("write BTC CSV");
        fs::write(btc_dir.join("BTCUSDT-1s-2024-02.csv"), "").expect("write BTC CSV");
        fs::write(eth_dir.join("ETHUSDT-1s-2024-01.csv"), "").expect("write ETH CSV");

        let files = collect_all_input_files(&test_dir.path, "1s").expect("collect all files");
        let groups = group_input_files_by_folder(&files);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[0][0].group, btc_dir);
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[1][0].group, eth_dir);
    }

    #[test]
    fn all_inspection_counts_rows_folder_by_folder() {
        let test_dir = TestDir::new();
        let btc_dir = test_dir.path.join("BTCUSDT");
        let eth_dir = test_dir.path.join("ETHUSDT");
        fs::create_dir_all(&btc_dir).expect("create BTC directory");
        fs::create_dir_all(&eth_dir).expect("create ETH directory");
        let btc_january = btc_dir.join("BTCUSDT-1s-2024-01.csv");
        let btc_february = btc_dir.join("BTCUSDT-1s-2024-02.csv");
        let eth_january = eth_dir.join("ETHUSDT-1s-2024-01.csv");

        write_rows(&btc_january, &[kline_row("10", &[]), kline_row("11", &[])]);
        write_rows(&btc_february, &[kline_row("12", &[])]);
        write_rows(
            &eth_january,
            &[
                kline_row("20", &[]),
                kline_row("21", &[]),
                kline_row("22", &["extra"]),
            ],
        );

        let files = collect_all_input_files(&test_dir.path, "1s").expect("collect all files");
        let file_info = inspect_all_files_by_folder(
            &files,
            InspectionOptions {
                mode: OutputMode::Overwrite,
                selection_mode: SelectionMode::All,
                auto_inspect_mode: AutoInspectMode::LastRow,
                expected_columns: 12,
                rsi_column_count: 1,
                jobs: 2,
                skip_confirmation: true,
            },
        )
        .expect("inspect all files");

        assert_eq!(file_info.len(), 3);
        assert_eq!(sum_counted_rows(&file_info), Some(6));
        assert_eq!(file_info[0].file.group, btc_dir);
        assert_eq!(file_info[0].row_count, Some(2));
        assert_eq!(file_info[1].row_count, Some(1));
        assert_eq!(file_info[2].file.group, eth_dir);
        assert_eq!(file_info[2].row_count, Some(3));
        assert_eq!(file_info[2].max_columns, Some(13));
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
            auto: false,
            all: false,
            overwrite: true,
            copy: false,
            version_flag: false,
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
            auto: false,
            all: false,
            overwrite: false,
            copy: true,
            version_flag: false,
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
            auto: false,
            all: false,
            overwrite: true,
            copy: false,
            version_flag: false,
        })
        .expect("run across files");

        let february_rows = read_rows(&february);
        assert_eq!(february_rows[0][12].parse::<f64>().unwrap(), 50.0);

        let second_value = february_rows[1][12].parse::<f64>().unwrap();
        assert!((second_value - 83.33333333333333).abs() < 1e-12);
    }

    #[test]
    fn all_carry_resets_state_for_each_parent_directory_group() {
        let test_dir = TestDir::new();
        let btc_dir = test_dir.path.join("spot").join("BTCUSDT");
        let eth_dir = test_dir.path.join("spot").join("ETHUSDT");
        fs::create_dir_all(&btc_dir).expect("create BTC directory");
        fs::create_dir_all(&eth_dir).expect("create ETH directory");
        let btc = btc_dir.join("BTCUSDT-1s-2024-01.csv");
        let eth = eth_dir.join("ETHUSDT-1s-2024-01.csv");

        write_rows(&btc, &[kline_row("10", &[]), kline_row("11", &[])]);
        write_rows(&eth, &[kline_row("20", &[]), kline_row("21", &[])]);

        run(Cli {
            dir: test_dir.path.clone(),
            name: "BTCUSDT".to_owned(),
            interval: "1s".to_owned(),
            column: 12,
            windows: vec![1],
            fallback: "NA".to_owned(),
            no_carry: false,
            carry: true,
            jobs: 5,
            auto: false,
            all: true,
            overwrite: true,
            copy: false,
            version_flag: false,
        })
        .expect("run all carry mode");

        let btc_rows = read_rows(&btc);
        let eth_rows = read_rows(&eth);

        assert_eq!(btc_rows[0][12], "NA");
        assert_eq!(btc_rows[1][12], "100");
        assert_eq!(eth_rows[0][12], "NA");
        assert_eq!(eth_rows[1][12], "100");
    }

    #[test]
    fn auto_carry_analysis_uses_first_row_and_counts_selected_files() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &[]),
                kline_row("11", &["old-rsi-a", "old-rsi-b"]),
            ],
        );

        let stats = analyze_file_for_auto_first_row(&file_path, 12, 2).expect("analyze file");

        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.min_columns, Some(12));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(true));
    }

    #[test]
    fn auto_carry_analysis_skips_from_first_row_without_counting_rows() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &["old-rsi-a", "old-rsi-b"]),
                kline_row("11", &[]),
            ],
        );

        let stats = analyze_file_for_auto_first_row(&file_path, 12, 2).expect("analyze file");
        let file_info = FileInfo {
            file: InputFile {
                path: file_path,
                group: test_dir.path.clone(),
                year: 2024,
                month: 1,
            },
            row_count: stats.row_count,
            min_columns: stats.min_columns,
            max_columns: stats.max_columns,
            auto_processable: stats.auto_processable,
        };

        assert_eq!(stats.row_count, None);
        assert_eq!(stats.min_columns, Some(14));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(false));
        assert!(!file_info.is_auto_processable(12));
    }

    #[test]
    fn auto_no_carry_analysis_uses_last_row_to_accept_files() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &["old-rsi-a", "old-rsi-b"]),
                kline_row("11", &[]),
            ],
        );

        let stats = analyze_file_for_auto_last_row(&file_path, 12, 2).expect("analyze file");

        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.min_columns, Some(12));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(true));
    }

    #[test]
    fn auto_no_carry_analysis_uses_last_row_to_skip_files() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &[]),
                kline_row("11", &["old-rsi-a", "old-rsi-b"]),
            ],
        );

        let stats = analyze_file_for_auto_last_row(&file_path, 12, 2).expect("analyze file");

        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.min_columns, Some(12));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(false));
    }

    #[test]
    fn auto_no_carry_uses_last_row_for_skip_decision() {
        let test_dir = TestDir::new();
        let january = test_dir.path.join("SOLUSDT-1s-2024-01.csv");
        let february = test_dir.path.join("SOLUSDT-1s-2024-02.csv");

        write_rows(
            &january,
            &[
                kline_row("10", &["old-rsi-a", "old-rsi-b"]),
                kline_row("11", &[]),
            ],
        );
        write_rows(
            &february,
            &[
                kline_row("10", &[]),
                kline_row("11", &["old-rsi-a", "old-rsi-b"]),
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
            carry: false,
            jobs: 5,
            auto: true,
            all: false,
            overwrite: true,
            copy: false,
            version_flag: false,
        })
        .expect("run auto no-carry mode");

        let january_rows = read_rows(&january);
        let february_rows = read_rows(&february);

        assert_eq!(january_rows[0].len(), 13);
        assert!(!january_rows[0].contains(&"old-rsi-a".to_owned()));
        assert_eq!(february_rows[0].len(), 12);
        assert_eq!(february_rows[1].len(), 14);
        assert!(february_rows[1].contains(&"old-rsi-a".to_owned()));
    }

    #[test]
    fn auto_carry_skips_overwide_files_and_carries_only_selected_files() {
        let test_dir = TestDir::new();
        let january = test_dir.path.join("SOLUSDT-1s-2024-01.csv");
        let february = test_dir.path.join("SOLUSDT-1s-2024-02.csv");
        let march = test_dir.path.join("SOLUSDT-1s-2024-03.csv");

        write_rows(&january, &[kline_row("10", &[]), kline_row("11", &[])]);
        write_rows(
            &february,
            &[
                kline_row("10", &["old-rsi-a", "old-rsi-b"]),
                kline_row("12", &["old-rsi-c", "old-rsi-d"]),
            ],
        );
        write_rows(&march, &[kline_row("11", &[]), kline_row("13", &[])]);

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
            auto: true,
            all: false,
            overwrite: true,
            copy: false,
            version_flag: false,
        })
        .expect("run auto carry mode");

        let january_rows = read_rows(&january);
        let february_rows = read_rows(&february);
        let march_rows = read_rows(&march);

        assert_eq!(january_rows[0].len(), 13);
        assert_eq!(february_rows[0].len(), 14);
        assert!(february_rows[0].contains(&"old-rsi-a".to_owned()));
        assert_eq!(march_rows[0].len(), 13);
        assert_eq!(march_rows[0][12].parse::<f64>().unwrap(), 100.0);

        let second_march_value = march_rows[1][12].parse::<f64>().unwrap();
        assert_eq!(second_march_value, 100.0);
    }

    #[test]
    fn auto_carry_treats_blank_appended_columns_as_incomplete() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &["", ""]),
                kline_row("11", &["old-rsi-a", "old-rsi-b"]),
            ],
        );

        let stats = analyze_file_for_auto_first_row(&file_path, 12, 2).expect("analyze file");

        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.min_columns, Some(14));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(true));
    }

    #[test]
    fn auto_no_carry_treats_partially_filled_last_row_as_incomplete() {
        let test_dir = TestDir::new();
        let file_path = test_dir.path.join("SOLUSDT-1s-2024-01.csv");

        write_rows(
            &file_path,
            &[
                kline_row("10", &["old-rsi-a", "old-rsi-b"]),
                kline_row("11", &["old-rsi-a", ""]),
            ],
        );

        let stats = analyze_file_for_auto_last_row(&file_path, 12, 2).expect("analyze file");

        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.min_columns, Some(14));
        assert_eq!(stats.max_columns, Some(14));
        assert_eq!(stats.auto_processable, Some(true));
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
            auto: false,
            all: false,
            overwrite: true,
            copy: false,
            version_flag: false,
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
            .flexible(true)
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
            .flexible(true)
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

    // ── truncate_progress_name tests ─────────────────────────────────────────

    #[test]
    fn truncate_progress_name_short_name_unchanged() {
        let name = "SOLUSDT/SOLUSDT-1s-2024-01.csv";
        assert_eq!(name.chars().count(), 30);
        assert_eq!(truncate_progress_name(name), name);
    }

    #[test]
    fn truncate_progress_name_exactly_34_chars_unchanged() {
        // At exactly MAX_LEN characters the name must be returned verbatim.
        let name = "A".repeat(34);
        assert_eq!(truncate_progress_name(&name), name);
    }

    #[test]
    fn truncate_progress_name_35_chars_truncated_with_ellipsis() {
        let name = "A".repeat(35);
        let result = truncate_progress_name(&name);
        // Result must be 34 chars: 33 'A's + '…'
        assert_eq!(result.chars().count(), 34);
        assert!(result.ends_with('…'));
        assert_eq!(&result[..result.len() - '…'.len_utf8()], "A".repeat(33));
    }

    #[test]
    fn truncate_progress_name_long_name_truncated() {
        let name = "futures/linear/ETHUSDT/ETHUSDT-1s-2024-01.csv";
        assert!(name.chars().count() > 34);
        let result = truncate_progress_name(name);
        assert_eq!(result.chars().count(), 34);
        assert!(result.ends_with('…'));
    }

    #[test]
    fn truncate_progress_name_multibyte_boundary() {
        // Ensure truncation works correctly when the name contains multibyte
        // characters so that the char-count boundary is respected.
        let name = "α".repeat(35); // each 'α' is 2 bytes but 1 char
        let result = truncate_progress_name(&name);
        assert_eq!(result.chars().count(), 34);
        assert!(result.ends_with('…'));
    }

    // ── format_with_commas tests ─────────────────────────────────────────────

    #[test]
    fn format_with_commas_zero() {
        assert_eq!(format_with_commas(0), "0");
    }

    #[test]
    fn format_with_commas_three_digits() {
        assert_eq!(format_with_commas(999), "999");
    }

    #[test]
    fn format_with_commas_four_digits() {
        assert_eq!(format_with_commas(1000), "1,000");
    }

    #[test]
    fn format_with_commas_seven_digits() {
        assert_eq!(format_with_commas(1_234_567), "1,234,567");
    }

    #[test]
    fn format_with_commas_boundary_999_999() {
        assert_eq!(format_with_commas(999_999), "999,999");
    }

    // ── format_rsi tests ─────────────────────────────────────────────────────

    #[test]
    fn format_rsi_zero_gain_zero_loss_returns_50() {
        assert_eq!(format_rsi(0.0, 0.0, "NA"), "50");
    }

    #[test]
    fn format_rsi_nonzero_gain_zero_loss_returns_100() {
        assert_eq!(format_rsi(1.0, 0.0, "NA"), "100");
    }

    #[test]
    fn format_rsi_zero_gain_nonzero_loss_returns_0() {
        let result = format_rsi(0.0, 1.0, "NA");
        let value: f64 = result.parse().expect("parseable float");
        assert!((value - 0.0).abs() < 1e-12);
    }

    #[test]
    fn format_rsi_equal_gain_and_loss_returns_50() {
        let result = format_rsi(1.0, 1.0, "NA");
        let value: f64 = result.parse().expect("parseable float");
        assert!((value - 50.0).abs() < 1e-12);
    }

    #[test]
    fn format_rsi_infinite_gain_returns_fallback() {
        assert_eq!(format_rsi(f64::INFINITY, 1.0, "NA"), "NA");
    }

    #[test]
    fn format_rsi_nan_loss_returns_fallback() {
        assert_eq!(format_rsi(1.0, f64::NAN, "NA"), "NA");
    }

    // ── validate_windows tests ───────────────────────────────────────────────

    #[test]
    fn validate_windows_rejects_empty_slice() {
        assert!(validate_windows(&[]).is_err());
    }

    #[test]
    fn validate_windows_rejects_zero_window() {
        assert!(validate_windows(&[0]).is_err());
        assert!(validate_windows(&[14, 0]).is_err());
    }

    #[test]
    fn validate_windows_accepts_valid_windows() {
        assert!(validate_windows(&[14]).is_ok());
        assert!(validate_windows(&[14, 64, 256]).is_ok());
    }

    // ── RsiState unit tests ──────────────────────────────────────────────────

    #[test]
    fn rsi_state_first_close_returns_fallback() {
        let mut state = RsiState::new(2);
        assert_eq!(state.next_cell(Some(100.0), "NA"), "NA");
    }

    #[test]
    fn rsi_state_non_finite_close_returns_fallback() {
        let mut state = RsiState::new(2);
        state.next_cell(Some(100.0), "NA"); // seed last_close
        assert_eq!(state.next_cell(Some(f64::NAN), "NA"), "NA");
        assert_eq!(state.next_cell(Some(f64::INFINITY), "NA"), "NA");
        assert_eq!(state.next_cell(None, "NA"), "NA");
    }

    #[test]
    fn rsi_state_window_2_pure_gains() {
        let mut state = RsiState::new(2);
        state.next_cell(Some(10.0), "NA"); // sets last_close, no output
        state.next_cell(Some(11.0), "NA"); // seed_count=1 < window=2, returns fallback
        let result = state.next_cell(Some(12.0), "NA"); // seed_count reaches 2, emits RSI
        let value: f64 = result.parse().expect("parseable float");
        assert_eq!(value, 100.0); // only gains, loss=0
    }

    #[test]
    fn rsi_state_window_2_pure_losses() {
        let mut state = RsiState::new(2);
        state.next_cell(Some(12.0), "NA");
        state.next_cell(Some(11.0), "NA");
        let result = state.next_cell(Some(10.0), "NA");
        let value: f64 = result.parse().expect("parseable float");
        assert!((value - 0.0).abs() < 1e-12);
    }

    #[test]
    fn rsi_state_wilder_smoothing_after_seed() {
        // After the seed period, subsequent values use the Wilder smoothing
        // formula: avg_gain = (prev_avg_gain * (n-1) + gain) / n
        let mut state = RsiState::new(2);
        // Sets last_close=10; no previous close yet → fallback
        state.next_cell(Some(10.0), "NA");
        // +1 gain, seed_count=1 < window=2 → fallback
        state.next_cell(Some(11.0), "NA");
        // +1 gain, seed_count=2 == window=2 → seed complete:
        //   avg_gain = (1+1)/2 = 1.0, avg_loss = 0/2 = 0.0 → RSI=100
        state.next_cell(Some(12.0), "NA");
        // Wilder step: 12→10, change=-2, gain=0, loss=2
        //   new_avg_gain = (1.0*(2-1) + 0) / 2 = 0.5
        //   new_avg_loss = (0.0*(2-1) + 2) / 2 = 1.0
        //   RS = 0.5 / 1.0 = 0.5  →  RSI = 100 - 100/1.5 ≈ 33.333…
        let result = state.next_cell(Some(10.0), "NA");
        let value: f64 = result.parse().expect("parseable float");
        let expected = 100.0 - 100.0 / (1.0 + 0.5);
        assert!((value - expected).abs() < 1e-12);
    }
}

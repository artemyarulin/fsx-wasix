#![cfg_attr(target_os = "wasi", feature(wasi_ext))]
// vim: tw=80
mod http;
mod tester_fsx;
mod tester_multi_threaded;
mod tester_oracle;
mod wss;

use std::{
    ffi::OsStr,
    io::Write,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process,
};

use clap::{builder::TypedValueParser, error::ErrorKind, Arg, Command, Error, Parser};
use clap_verbosity_flag::{Verbosity, WarnLevel};
use tester_fsx::Config;

#[derive(Clone)]
struct MonitorParser {}
impl TypedValueParser for MonitorParser {
    type Value = (u64, u64);

    fn parse_ref(
        &self,
        cmd: &Command,
        _arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let vs = value
            .to_str()
            .ok_or_else(|| clap::Error::new(ErrorKind::InvalidUtf8).with_cmd(cmd))?;
        let fields = vs.split(':').collect::<Vec<_>>();
        if fields.len() != 2 {
            let e = clap::Error::raw(
                ErrorKind::InvalidValue,
                "-m argument must contain exactly one ':'",
            )
            .with_cmd(cmd);
            return Err(e);
        }
        let startop = fields[0].parse::<u64>().map_err(|_| {
            clap::Error::raw(ErrorKind::InvalidValue, "-m arguments must be numeric")
        })?;
        let endop = fields[1].parse::<u64>().map_err(|_| {
            clap::Error::raw(ErrorKind::InvalidValue, "-m arguments must be numeric")
        })?;
        Ok((startop, endop))
    }
}

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct Cli {
    /// Beginning operation number
    #[arg(short = 'b', default_value_t = NonZeroU64::new(1u64).unwrap())]
    pub(crate) opnum: NonZeroU64,

    /// Config file path
    #[arg(short = 'f', value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,

    /// Monitor specified byte range
    #[arg(short = 'm', value_name = "FROM:TO", value_parser = MonitorParser{})]
    pub(crate) monitor: Option<(u64, u64)>,

    /// Total number of operations to do [default infinity]
    #[arg(short = 'N')]
    pub(crate) numops: Option<u64>,

    /// Save artifacts to this directory [default ./]
    #[arg(short = 'P', value_name = "DIRPATH")]
    pub(crate) artifacts_dir: Option<PathBuf>,

    /// Seed for RNG
    #[arg(short = 'S')]
    pub(crate) seed: Option<u64>,

    /// Worker threads [default: logical CPU count]
    #[arg(short = 'j', long = "threads", value_name = "N")]
    pub(crate) threads: Option<NonZeroUsize>,

    /// Run an HTTP server instead of a one-shot fsx run [default port: $PORT or 8080]
    #[arg(long = "server", value_name = "PORT", num_args = 0..=1)]
    pub(crate) server: Option<Option<u16>>,

    /// Run the deterministic coordinator-driven multi-threaded tester
    #[arg(long)]
    pub(crate) orchestrated: bool,

    /// Run the exhaustive native-oracle command-sequence tester
    #[arg(long)]
    pub(crate) oracle: bool,

    /// Generate deterministic theory trial fixture files into this directory
    #[arg(long = "oracle-prepare-fixtures", value_name = "DIR")]
    pub(crate) oracle_prepare_fixtures: Option<PathBuf>,

    /// Directory containing deterministic theory trial fixture files
    #[arg(long = "oracle-fixtures", value_name = "DIR")]
    pub(crate) oracle_fixtures: Option<PathBuf>,

    /// Write oracle transcript to this path
    #[arg(long = "oracle-output")]
    pub(crate) oracle_output: Option<PathBuf>,

    /// Compare oracle transcript against this path
    #[arg(long = "oracle-expected")]
    pub(crate) oracle_expected: Option<PathBuf>,

    /// First deterministic oracle case id to run (1-based)
    #[arg(long = "case-start")]
    pub(crate) oracle_case_start: Option<usize>,

    /// Number of deterministic oracle cases to run from --case-start
    #[arg(long = "case-count")]
    pub(crate) oracle_case_count: Option<usize>,

    /// Random oracle case sample count (use -S to replay a specific sample seed)
    #[arg(long = "oracle-sample-count")]
    pub(crate) oracle_sample_count: Option<usize>,

    /// Re-read Wasmer volume files externally and compare them against native oracle snapshots
    #[arg(
        long = "oracle-verify-files",
        value_names = ["NATIVE_REPORT", "WASIX_REPORT"],
        num_args = 2
    )]
    pub(crate) oracle_verify_files: Option<Vec<PathBuf>>,

    /// Print the active oracle operation catalog and cache key
    #[arg(long = "oracle-catalog")]
    pub(crate) oracle_catalog: bool,

    /// Print only the active oracle cache key
    #[arg(long = "oracle-catalog-key")]
    pub(crate) oracle_catalog_key: bool,

    /// Print a compact summary of enabled oracle I/O operations
    #[arg(long = "oracle-catalog-syscalls")]
    pub(crate) oracle_catalog_syscalls: bool,

    /// Orchestrated scenario name
    #[arg(long, default_value = "shared-inode")]
    pub(crate) scenario: String,

    /// Orchestrated logical file count
    #[arg(long, default_value_t = NonZeroUsize::new(2).unwrap())]
    pub(crate) files: NonZeroUsize,

    /// Orchestrated max handles per worker
    #[arg(long, default_value_t = NonZeroUsize::new(8).unwrap())]
    pub(crate) handles: NonZeroUsize,

    /// Orchestrated max commands released in a ParallelGroup
    #[arg(long, default_value_t = NonZeroUsize::new(1).unwrap())]
    pub(crate) parallelism: NonZeroUsize,

    /// Orchestrated fresh verification cadence in coordinator steps
    #[arg(long = "verify-every")]
    pub(crate) verify_every: Option<NonZeroU64>,

    /// Orchestrated manifest path for persisted verification
    #[arg(long)]
    pub(crate) manifest: Option<PathBuf>,

    /// File name to operate on
    #[arg(required_unless_present_any = ["server", "oracle_verify_files", "oracle_catalog", "oracle_catalog_key", "oracle_catalog_syscalls", "oracle_prepare_fixtures"])]
    pub(crate) fname: Option<PathBuf>,

    /// Inject an error on step N
    // This option mainly exists just for the sake of the integration tests.
    #[arg(long = "inject", hide = true, value_name = "N")]
    pub(crate) inject: Option<u64>,

    #[command(flatten)]
    pub(crate) verbose: Verbosity<WarnLevel>,
}

fn main() {
    let cli = Cli::parse();
    env_logger::builder()
        .filter_level(cli.verbose.log_level_filter())
        .format(|buf, record| writeln!(buf, "[{:<5} fsx] {}", record.level(), record.args()))
        .format_timestamp(None)
        .init();
    let config = cli.config.as_ref().map(Config::load).unwrap_or_default();
    if let Some(path) = &cli.oracle_prepare_fixtures {
        if let Err(e) = tester_oracle::prepare_theory_trial_fixtures(path) {
            eprintln!("error: {e}");
            process::exit(1);
        }
    } else if cli.oracle_catalog_key {
        println!("{}", tester_oracle::catalog_key());
    } else if cli.oracle_catalog_syscalls {
        println!("{}", tester_oracle::catalog_syscalls());
    } else if cli.oracle_catalog {
        print!("{}", tester_oracle::catalog_report());
    } else if let Some(reports) = &cli.oracle_verify_files {
        if let Err(e) = tester_oracle::verify_files(&reports[0], &reports[1]) {
            eprintln!("error: {e}");
            process::exit(1);
        }
    } else if cli.server.is_some() {
        if let Err(e) = http::run_server(cli, config) {
            eprintln!("error: server failed: {e}");
            process::exit(1);
        }
    } else {
        if cli.oracle {
            if let Err(e) = tester_oracle::run(cli) {
                eprintln!("error: {e}");
                process::exit(1);
            }
        } else if cli.orchestrated {
            if let Err(e) = tester_multi_threaded::run_workers(cli, config) {
                eprintln!("error: {e}");
                process::exit(1);
            }
        } else {
            config.validate(&cli);
            if let Err(e) = tester_fsx::run_workers(cli, config) {
                eprintln!("error: {e}");
                process::exit(1);
            }
        }
    }
}

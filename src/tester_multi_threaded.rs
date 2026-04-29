use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use crate::{tester_fsx::Config, Cli};

type HandleId = u64;

#[derive(Clone, Copy, Debug)]
struct OpenFlags {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
}

impl OpenFlags {
    fn read_only() -> Self {
        OpenFlags {
            read: true,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
        }
    }

    fn read_write_create() -> Self {
        OpenFlags {
            read: true,
            write: true,
            append: false,
            truncate: false,
            create: true,
            create_new: false,
        }
    }

    fn read_write_truncate() -> Self {
        OpenFlags {
            read: true,
            write: true,
            append: false,
            truncate: true,
            create: true,
            create_new: false,
        }
    }

    fn append_create() -> Self {
        OpenFlags {
            read: true,
            write: false,
            append: true,
            truncate: false,
            create: true,
            create_new: false,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum LogSink {
    Stderr,
    Handle(usize, HandleId),
}

#[allow(dead_code)]
#[derive(Debug)]
enum WorkerCommand {
    Open {
        step: u64,
        handle: HandleId,
        path: PathBuf,
        flags: OpenFlags,
    },
    Close {
        step: u64,
        handle: HandleId,
    },
    ReadAt {
        step: u64,
        handle: HandleId,
        offset: u64,
        size: usize,
    },
    WriteAt {
        step: u64,
        handle: HandleId,
        offset: u64,
        data: Vec<u8>,
    },
    Seek {
        step: u64,
        handle: HandleId,
        offset: u64,
    },
    ReadSeq {
        step: u64,
        handle: HandleId,
        size: usize,
    },
    WriteSeq {
        step: u64,
        handle: HandleId,
        data: Vec<u8>,
    },
    Fsync {
        step: u64,
        handle: HandleId,
    },
    Truncate {
        step: u64,
        handle: HandleId,
        size: u64,
    },
    Unlink {
        step: u64,
        path: PathBuf,
    },
    Rename {
        step: u64,
        from: PathBuf,
        to: PathBuf,
    },
    WriteStdout {
        step: u64,
        data: Vec<u8>,
    },
    WriteStderr {
        step: u64,
        data: Vec<u8>,
    },
    SwitchLogSink {
        step: u64,
        sink: LogSink,
    },
    WriteLog {
        step: u64,
        data: Vec<u8>,
    },
    VerifyPath {
        step: u64,
        path: PathBuf,
    },
    VerifyHandle {
        step: u64,
        handle: HandleId,
        offset: u64,
        size: usize,
    },
    Stop {
        step: u64,
    },
}

impl WorkerCommand {
    fn step(&self) -> u64 {
        match self {
            WorkerCommand::Open { step, .. }
            | WorkerCommand::Close { step, .. }
            | WorkerCommand::ReadAt { step, .. }
            | WorkerCommand::WriteAt { step, .. }
            | WorkerCommand::Seek { step, .. }
            | WorkerCommand::ReadSeq { step, .. }
            | WorkerCommand::WriteSeq { step, .. }
            | WorkerCommand::Fsync { step, .. }
            | WorkerCommand::Truncate { step, .. }
            | WorkerCommand::Unlink { step, .. }
            | WorkerCommand::Rename { step, .. }
            | WorkerCommand::WriteStdout { step, .. }
            | WorkerCommand::WriteStderr { step, .. }
            | WorkerCommand::SwitchLogSink { step, .. }
            | WorkerCommand::WriteLog { step, .. }
            | WorkerCommand::VerifyPath { step, .. }
            | WorkerCommand::VerifyHandle { step, .. }
            | WorkerCommand::Stop { step } => *step,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct WorkerReply {
    step: u64,
    worker: usize,
    raw_fd: Option<i64>,
    bytes: usize,
    data: Vec<u8>,
    error: Option<String>,
}

#[allow(dead_code)]
struct OpenHandle {
    file: File,
    path: PathBuf,
    pos: u64,
}

struct Worker {
    id: usize,
    handles: HashMap<HandleId, OpenHandle>,
    log_sink: LogSink,
}

impl Worker {
    fn new(id: usize) -> Self {
        Worker {
            id,
            handles: HashMap::new(),
            log_sink: LogSink::Stderr,
        }
    }

    fn make_reply(
        worker: usize,
        step: u64,
        raw_fd: Option<i64>,
        bytes: usize,
        data: Vec<u8>,
    ) -> WorkerReply {
        WorkerReply {
            step,
            worker,
            raw_fd,
            bytes,
            data,
            error: None,
        }
    }

    fn err(&self, step: u64, error: impl fmt::Display) -> WorkerReply {
        Self::make_err(self.id, step, error)
    }

    fn make_err(worker: usize, step: u64, error: impl fmt::Display) -> WorkerReply {
        WorkerReply {
            step,
            worker,
            raw_fd: None,
            bytes: 0,
            data: Vec::new(),
            error: Some(error.to_string()),
        }
    }

    fn handle_mut(&mut self, step: u64, handle: HandleId) -> Result<&mut OpenHandle, WorkerReply> {
        if self.handles.contains_key(&handle) {
            Ok(self.handles.get_mut(&handle).unwrap())
        } else {
            Err(self.err(step, format!("unknown handle {handle}")))
        }
    }

    fn execute(&mut self, cmd: WorkerCommand) -> WorkerReply {
        let worker_id = self.id;
        match cmd {
            WorkerCommand::Open {
                step,
                handle,
                path,
                flags,
            } => {
                if let Some(parent) = path.parent() {
                    if let Err(e) = fs::create_dir_all(parent) {
                        return Worker::make_err(worker_id, step, e);
                    }
                }
                if flags.create && !path.exists() {
                    if let Err(e) = File::create(&path) {
                        return Worker::make_err(worker_id, step, e);
                    }
                }
                let mut oo = OpenOptions::new();
                oo.read(flags.read)
                    .write(flags.write)
                    .append(flags.append)
                    .truncate(flags.truncate)
                    .create(flags.create)
                    .create_new(flags.create_new);
                match oo.open(&path) {
                    Ok(file) => {
                        let raw_fd = Some(file.as_raw_fd() as i64);
                        self.handles
                            .insert(handle, OpenHandle { file, path, pos: 0 });
                        Worker::make_reply(worker_id, step, raw_fd, 0, Vec::new())
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Close { step, handle } => {
                if self.handles.remove(&handle).is_some() {
                    Worker::make_reply(worker_id, step, None, 0, Vec::new())
                } else {
                    Worker::make_err(worker_id, step, format!("unknown handle {handle}"))
                }
            }
            WorkerCommand::ReadAt {
                step,
                handle,
                offset,
                size,
            } => {
                let worker_id = self.id;
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                if let Err(e) = h.file.seek(SeekFrom::Start(offset)) {
                    return Worker::make_err(worker_id, step, e);
                }
                let mut data = vec![0; size];
                match h.file.read(&mut data) {
                    Ok(n) => {
                        data.truncate(n);
                        Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            n,
                            data,
                        )
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::WriteAt {
                step,
                handle,
                offset,
                data,
            } => {
                let worker_id = self.id;
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                if let Err(e) = h.file.seek(SeekFrom::Start(offset)) {
                    return Worker::make_err(worker_id, step, e);
                }
                match h.file.write_all(&data) {
                    Ok(()) => Worker::make_reply(
                        worker_id,
                        step,
                        Some(h.file.as_raw_fd() as i64),
                        data.len(),
                        Vec::new(),
                    ),
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Seek {
                step,
                handle,
                offset,
            } => {
                let worker_id = self.id;
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                match h.file.seek(SeekFrom::Start(offset)) {
                    Ok(pos) => {
                        h.pos = pos;
                        Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            0,
                            Vec::new(),
                        )
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::ReadSeq { step, handle, size } => {
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                let mut data = vec![0; size];
                match h.file.read(&mut data) {
                    Ok(n) => {
                        data.truncate(n);
                        h.pos += n as u64;
                        Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            n,
                            data,
                        )
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::WriteSeq { step, handle, data } => {
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                match h.file.write_all(&data) {
                    Ok(()) => {
                        h.pos += data.len() as u64;
                        Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            data.len(),
                            Vec::new(),
                        )
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Fsync { step, handle } => {
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                match h.file.sync_all() {
                    Ok(()) => Worker::make_reply(
                        worker_id,
                        step,
                        Some(h.file.as_raw_fd() as i64),
                        0,
                        Vec::new(),
                    ),
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Truncate { step, handle, size } => {
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                match h.file.set_len(size) {
                    Ok(()) => Worker::make_reply(
                        worker_id,
                        step,
                        Some(h.file.as_raw_fd() as i64),
                        0,
                        Vec::new(),
                    ),
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Unlink { step, path } => match fs::remove_file(path) {
                Ok(()) => Worker::make_reply(worker_id, step, None, 0, Vec::new()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Worker::make_reply(worker_id, step, None, 0, Vec::new())
                }
                Err(e) => Worker::make_err(worker_id, step, e),
            },
            WorkerCommand::Rename { step, from, to } => match fs::rename(from, to) {
                Ok(()) => Worker::make_reply(worker_id, step, None, 0, Vec::new()),
                Err(e) => Worker::make_err(worker_id, step, e),
            },
            WorkerCommand::WriteStdout { step, data } => {
                let mut out = std::io::stdout();
                match out.write_all(&data).and_then(|_| out.flush()) {
                    Ok(()) => Worker::make_reply(worker_id, step, None, data.len(), Vec::new()),
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::WriteStderr { step, data } => {
                let mut out = std::io::stderr();
                match out.write_all(&data).and_then(|_| out.flush()) {
                    Ok(()) => Worker::make_reply(worker_id, step, None, data.len(), Vec::new()),
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::SwitchLogSink { step, sink } => {
                self.log_sink = sink;
                Worker::make_reply(worker_id, step, None, 0, Vec::new())
            }
            WorkerCommand::WriteLog { step, data } => match self.log_sink.clone() {
                LogSink::Stderr => {
                    let mut out = std::io::stderr();
                    match out.write_all(&data).and_then(|_| out.flush()) {
                        Ok(()) => Worker::make_reply(worker_id, step, None, data.len(), Vec::new()),
                        Err(e) => Worker::make_err(worker_id, step, e),
                    }
                }
                LogSink::Handle(_, handle) => {
                    let h = match self.handle_mut(step, handle) {
                        Ok(h) => h,
                        Err(e) => return e,
                    };
                    match h.file.write_all(&data) {
                        Ok(()) => Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            data.len(),
                            Vec::new(),
                        ),
                        Err(e) => Worker::make_err(worker_id, step, e),
                    }
                }
            },
            WorkerCommand::VerifyPath { step, path } => match fs::read(path) {
                Ok(data) => Worker::make_reply(worker_id, step, None, data.len(), data),
                Err(e) => Worker::make_err(worker_id, step, e),
            },
            WorkerCommand::VerifyHandle {
                step,
                handle,
                offset,
                size,
            } => {
                let h = match self.handle_mut(step, handle) {
                    Ok(h) => h,
                    Err(e) => return e,
                };
                if let Err(e) = h.file.seek(SeekFrom::Start(offset)) {
                    return Worker::make_err(worker_id, step, e);
                }
                let mut data = vec![0; size];
                match h.file.read(&mut data) {
                    Ok(n) => {
                        data.truncate(n);
                        Worker::make_reply(
                            worker_id,
                            step,
                            Some(h.file.as_raw_fd() as i64),
                            n,
                            data,
                        )
                    }
                    Err(e) => Worker::make_err(worker_id, step, e),
                }
            }
            WorkerCommand::Stop { step } => {
                Worker::make_reply(worker_id, step, None, 0, Vec::new())
            }
        }
    }
}

#[derive(Clone, Debug)]
struct HandleMeta {
    path: PathBuf,
    pos: u64,
}

struct ExpectedWorld {
    root: PathBuf,
    files: BTreeMap<PathBuf, Vec<u8>>,
    handles: HashMap<(usize, HandleId), HandleMeta>,
    command_log: Vec<String>,
    verified_files: usize,
}

impl ExpectedWorld {
    fn new(root: PathBuf) -> Self {
        ExpectedWorld {
            root,
            files: BTreeMap::new(),
            handles: HashMap::new(),
            command_log: Vec::new(),
            verified_files: 0,
        }
    }

    fn log(&mut self, line: impl Into<String>) {
        self.command_log.push(line.into());
        if self.command_log.len() > 512 {
            self.command_log.remove(0);
        }
    }

    fn open(&mut self, worker: usize, handle: HandleId, path: PathBuf, flags: OpenFlags) {
        if flags.truncate {
            self.files.insert(path.clone(), Vec::new());
        } else if flags.create || flags.create_new {
            self.files.entry(path.clone()).or_default();
        }
        self.handles
            .insert((worker, handle), HandleMeta { path, pos: 0 });
    }

    fn close(&mut self, worker: usize, handle: HandleId) {
        self.handles.remove(&(worker, handle));
    }

    fn handle_path(&self, worker: usize, handle: HandleId) -> Result<PathBuf, RunError> {
        self.handles
            .get(&(worker, handle))
            .map(|h| h.path.clone())
            .ok_or_else(|| RunError::new("syscall-error", format!("unknown handle {handle}")))
    }

    fn set_pos(&mut self, worker: usize, handle: HandleId, pos: u64) {
        if let Some(h) = self.handles.get_mut(&(worker, handle)) {
            h.pos = pos;
        }
    }

    fn write_at(
        &mut self,
        worker: usize,
        handle: HandleId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), RunError> {
        let path = self.handle_path(worker, handle)?;
        let buf = self.files.entry(path).or_default();
        let start = offset as usize;
        if buf.len() < start {
            buf.resize(start, 0);
        }
        if buf.len() < start + data.len() {
            buf.resize(start + data.len(), 0);
        }
        buf[start..start + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn append(&mut self, worker: usize, handle: HandleId, data: &[u8]) -> Result<(), RunError> {
        let path = self.handle_path(worker, handle)?;
        self.files.entry(path).or_default().extend_from_slice(data);
        Ok(())
    }

    #[allow(dead_code)]
    fn truncate(&mut self, worker: usize, handle: HandleId, size: u64) -> Result<(), RunError> {
        let path = self.handle_path(worker, handle)?;
        self.files.entry(path).or_default().resize(size as usize, 0);
        Ok(())
    }

    fn unlink(&mut self, path: &Path) {
        self.files.remove(path);
    }

    fn rename(&mut self, from: &Path, to: &Path) {
        if let Some(data) = self.files.remove(from) {
            self.files.insert(to.to_path_buf(), data);
        }
        for meta in self.handles.values_mut() {
            if meta.path == from {
                meta.path = to.to_path_buf();
            }
        }
    }

    fn verify_path(&mut self, path: &Path, actual: &[u8]) -> Result<(), RunError> {
        self.verified_files += 1;
        let Some(expected) = self.files.get(path) else {
            return Err(RunError::new(
                "unexpected-file",
                format!("{} exists but is not modeled", path.display()),
            ));
        };
        compare_bytes(path, expected, actual)
    }

    fn verify_all_fresh(&mut self) -> Result<(), RunError> {
        let entries = self.files.clone();
        for (path, expected) in entries {
            let actual = fs::read(&path).map_err(|e| {
                RunError::new(
                    "missing-file",
                    format!("failed to read {}: {e}", path.display()),
                )
            })?;
            self.verified_files += 1;
            compare_bytes(&path, &expected, &actual)?;
        }

        let modeled = self.files.keys().cloned().collect::<HashSet<_>>();
        for path in collect_files(&self.root)? {
            if !modeled.contains(&path) {
                return Err(RunError::new(
                    "unexpected-file",
                    format!("unmodeled file under run root: {}", path.display()),
                ));
            }
        }
        Ok(())
    }

    fn write_manifest(&self, path: &Path) -> Result<(), RunError> {
        let mut out = String::new();
        for (file, data) in &self.files {
            out.push_str(&format!(
                "{}\t{}\t{:016x}\n",
                file.strip_prefix(&self.root).unwrap_or(file).display(),
                data.len(),
                stable_hash(data)
            ));
        }
        fs::write(path, out).map_err(|e| {
            RunError::new(
                "syscall-error",
                format!("failed to write manifest {}: {e}", path.display()),
            )
        })
    }

    fn report(&self, err: &RunError, scenario: &str, seed: u64) -> String {
        let mut out = format!(
            "category={}\nscenario={scenario}\nseed={seed}\nmessage={}\nLOG DUMP\n",
            err.category, err.message
        );
        for line in &self.command_log {
            out.push_str(line);
            out.push('\n');
        }
        out
    }
}

#[derive(Debug)]
pub(crate) struct RunError {
    category: &'static str,
    message: String,
    report: String,
}

impl RunError {
    fn new(category: &'static str, message: impl fmt::Display) -> Self {
        RunError {
            category,
            message: message.to_string(),
            report: String::new(),
        }
    }

    fn with_report(mut self, report: String) -> Self {
        self.report = report;
        self
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.report.is_empty() {
            write!(f, "{}: {}", self.category, self.message)
        } else {
            write!(f, "{}: {}\n{}", self.category, self.message, self.report)
        }
    }
}

#[derive(Clone, Debug)]
struct RunConfig {
    root: PathBuf,
    scenario: String,
    workers: usize,
    files: usize,
    handles: usize,
    parallelism: usize,
    verify_every: Option<u64>,
    numops: u64,
    seed: u64,
    flen: usize,
    opsize_min: usize,
    opsize_max: usize,
    manifest: Option<PathBuf>,
    inject: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct RunSummary {
    pub(crate) workers: usize,
    pub(crate) elapsed: Duration,
    pub(crate) steps: u64,
    pub(crate) verified_files: usize,
    pub(crate) scenario: String,
}

pub(crate) fn worker_count(cli: &Cli) -> usize {
    cli.threads.map(NonZeroUsize::get).unwrap_or_else(|| {
        thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1)
    })
}

pub(crate) fn run_workers(cli: Cli, config: Config) -> Result<RunSummary, String> {
    run(cli, config).map_err(|e| e.to_string())
}

pub(crate) fn run(cli: Cli, config: Config) -> Result<RunSummary, RunError> {
    let started = Instant::now();
    let cfg = RunConfig::from_cli(&cli, &config)?;
    prepare_root(&cfg.root)?;

    if cfg.scenario == "verify-manifest" {
        verify_manifest(&cfg)?;
        return Ok(RunSummary {
            workers: cfg.workers,
            elapsed: started.elapsed(),
            steps: 0,
            verified_files: 0,
            scenario: cfg.scenario,
        });
    }

    let mut coordinator = Coordinator::new(cfg.clone())?;
    let result = coordinator.run();
    coordinator.stop_workers();

    match result {
        Ok(()) => {
            if let Some(manifest) = &cfg.manifest {
                coordinator.world.write_manifest(manifest)?;
            }
            Ok(RunSummary {
                workers: cfg.workers,
                elapsed: started.elapsed(),
                steps: coordinator.steps,
                verified_files: coordinator.world.verified_files,
                scenario: cfg.scenario,
            })
        }
        Err(e) => {
            let report = coordinator.world.report(&e, &cfg.scenario, cfg.seed);
            Err(e.with_report(report))
        }
    }
}

impl RunConfig {
    fn from_cli(cli: &Cli, config: &Config) -> Result<Self, RunError> {
        let root = cli
            .fname
            .clone()
            .ok_or_else(|| RunError::new("syscall-error", "run root is required"))?;
        let opsize_min = config.opsize.min;
        let opsize_max = config.opsize.max.max(opsize_min.max(1));
        Ok(RunConfig {
            root,
            scenario: cli.scenario.clone(),
            workers: worker_count(cli).max(1),
            files: cli.files.get().max(1),
            handles: cli.handles.get().max(1),
            parallelism: cli.parallelism.get().max(1),
            verify_every: cli.verify_every.map(Into::into),
            numops: cli.numops.unwrap_or(32).max(1),
            seed: cli.seed.unwrap_or(1),
            flen: config.flen.map(|v| v as usize).unwrap_or(256 * 1024),
            opsize_min,
            opsize_max,
            manifest: cli.manifest.clone(),
            inject: cli.inject,
        })
    }
}

struct Coordinator {
    cfg: RunConfig,
    world: ExpectedWorld,
    senders: Vec<mpsc::Sender<WorkerCommand>>,
    reply_rx: mpsc::Receiver<WorkerReply>,
    handles: Vec<thread::JoinHandle<()>>,
    steps: u64,
    rng: XorShiftRng,
}

impl Coordinator {
    fn new(cfg: RunConfig) -> Result<Self, RunError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        let mut senders = Vec::with_capacity(cfg.workers);
        let mut handles = Vec::with_capacity(cfg.workers);
        for worker_id in 1..=cfg.workers {
            let (tx, rx) = mpsc::channel();
            let replies = reply_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("orchestrated-worker-{worker_id}"))
                .spawn(move || {
                    let mut worker = Worker::new(worker_id);
                    while let Ok(cmd) = rx.recv() {
                        let stop = matches!(cmd, WorkerCommand::Stop { .. });
                        let reply = worker.execute(cmd);
                        let _ = replies.send(reply);
                        if stop {
                            break;
                        }
                    }
                })
                .map_err(|e| {
                    RunError::new(
                        "syscall-error",
                        format!("failed to spawn worker {worker_id}: {e}"),
                    )
                })?;
            senders.push(tx);
            handles.push(handle);
        }
        let seed = cfg.seed;
        Ok(Coordinator {
            world: ExpectedWorld::new(cfg.root.clone()),
            cfg,
            senders,
            reply_rx,
            handles,
            steps: 0,
            rng: XorShiftRng::seed_from_u64(seed),
        })
    }

    fn run(&mut self) -> Result<(), RunError> {
        match self.cfg.scenario.as_str() {
            "random" => self.scenario_random(),
            "shared-inode" | "same-file-positioned-io" => self.scenario_shared_inode(),
            "same-file-independent-offsets" => self.scenario_independent_offsets(),
            "same-file-overlapping-writes" => self.scenario_overlapping_writes(),
            "stronger-rights" | "read-handle-survives-write-open" | "stronger-rights-reopen" => {
                self.scenario_stronger_rights()
            }
            "fd-reuse" | "fd-reuse-close-after-open" | "fd-reuse-many-fillers" => {
                self.scenario_fd_reuse()
            }
            "fd-try-clone-shape" | "fd-renumber-shape" | "close-while-peer-reads" => {
                self.scenario_fd_reuse()
            }
            "append" | "append-serial" | "append-parallel-pairs" => self.scenario_append(),
            "append-after-stale-size-pressure" | "append-large-records" => self.scenario_append(),
            "unlink-recreate" | "unlink-open-handle" | "unlink-recreate-fd-reuse" => {
                self.scenario_unlink_recreate()
            }
            "rename-open-handle" | "rename-replace-existing" | "symlink-alias-same-file" => {
                self.scenario_rename()
            }
            "wordpress-like"
            | "wp-render-read-set"
            | "wp-two-request"
            | "wp-debug-log-switch"
            | "wp-plugin-rewrite-while-included"
            | "wp-shutdown-function"
            | "wp-client-abort-shape"
            | "stderr-pressure"
            | "log-file-switch"
            | "stdio-close-open-churn"
            | "log-handle-left-open" => self.scenario_wordpress_like(),
            "fresh-open-after-every-write"
            | "close-all-final-verify"
            | "cross-worker-final-verify" => self.scenario_shared_inode(),
            "restart-compatible-manifest" => self.scenario_shared_inode(),
            "inject-wrong-byte" | "inject-wrong-file" | "inject-short-write-model" => {
                self.scenario_injection()
            }
            "timeout-with-open-handles" => self.scenario_shared_inode(),
            other => Err(RunError::new(
                "syscall-error",
                format!("unknown scenario {other}"),
            )),
        }?;
        self.world.verify_all_fresh()
    }

    fn stop_workers(&mut self) {
        for i in 0..self.senders.len() {
            let _ = self.senders[i].send(WorkerCommand::Stop {
                step: self.steps + 1,
            });
        }
        for _ in 0..self.senders.len() {
            let _ = self.reply_rx.recv();
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }

    fn next_step(&mut self) -> u64 {
        self.steps += 1;
        self.steps
    }

    fn path(&self, id: usize) -> PathBuf {
        self.cfg.root.join(format!("file-{id}.dat"))
    }

    fn worker_idx(&self, worker: usize) -> usize {
        worker.saturating_sub(1) % self.senders.len()
    }

    fn send(&mut self, worker: usize, cmd: WorkerCommand) -> Result<WorkerReply, RunError> {
        let step = cmd.step();
        self.world
            .log(format!("{step}: worker={worker} cmd={cmd:?}"));
        self.senders[self.worker_idx(worker)]
            .send(cmd)
            .map_err(|e| RunError::new("syscall-error", e))?;
        let reply = self
            .reply_rx
            .recv()
            .map_err(|e| RunError::new("syscall-error", e))?;
        self.check_reply(reply)
    }

    fn send_group(
        &mut self,
        commands: Vec<(usize, WorkerCommand)>,
    ) -> Result<Vec<WorkerReply>, RunError> {
        let mut expected = HashSet::new();
        for (worker, cmd) in commands {
            expected.insert(cmd.step());
            self.world
                .log(format!("{}: worker={worker} cmd={cmd:?}", cmd.step()));
            self.senders[self.worker_idx(worker)]
                .send(cmd)
                .map_err(|e| RunError::new("syscall-error", e))?;
        }
        let mut replies = Vec::with_capacity(expected.len());
        while !expected.is_empty() {
            let reply = self
                .reply_rx
                .recv()
                .map_err(|e| RunError::new("syscall-error", e))?;
            expected.remove(&reply.step);
            replies.push(self.check_reply(reply)?);
        }
        Ok(replies)
    }

    fn check_reply(&self, reply: WorkerReply) -> Result<WorkerReply, RunError> {
        if let Some(e) = &reply.error {
            Err(RunError::new(
                "syscall-error",
                format!("step {} worker {}: {e}", reply.step, reply.worker),
            ))
        } else {
            Ok(reply)
        }
    }

    fn maybe_verify(&mut self) -> Result<(), RunError> {
        if let Some(every) = self.cfg.verify_every {
            if every > 0 && self.steps % every == 0 {
                self.world.verify_all_fresh()?;
            }
        }
        Ok(())
    }

    fn open(
        &mut self,
        worker: usize,
        handle: HandleId,
        path: PathBuf,
        flags: OpenFlags,
    ) -> Result<(), RunError> {
        let step = self.next_step();
        self.send(
            worker,
            WorkerCommand::Open {
                step,
                handle,
                path: path.clone(),
                flags,
            },
        )?;
        self.world.open(worker, handle, path, flags);
        self.maybe_verify()
    }

    fn close(&mut self, worker: usize, handle: HandleId) -> Result<(), RunError> {
        let step = self.next_step();
        self.send(worker, WorkerCommand::Close { step, handle })?;
        self.world.close(worker, handle);
        self.maybe_verify()
    }

    fn write_at(
        &mut self,
        worker: usize,
        handle: HandleId,
        offset: u64,
        expected: Vec<u8>,
    ) -> Result<(), RunError> {
        let step = self.next_step();
        let mut actual = expected.clone();
        if self.cfg.inject == Some(step) && !actual.is_empty() {
            actual[0] ^= 0xff;
        }
        self.send(
            worker,
            WorkerCommand::WriteAt {
                step,
                handle,
                offset,
                data: actual,
            },
        )?;
        self.world.write_at(worker, handle, offset, &expected)?;
        self.maybe_verify()
    }

    fn read_at_verify(
        &mut self,
        worker: usize,
        handle: HandleId,
        offset: u64,
        size: usize,
    ) -> Result<(), RunError> {
        let path = self.world.handle_path(worker, handle)?;
        let expected = self
            .world
            .files
            .get(&path)
            .map(|b| {
                b.get(offset as usize..(offset as usize + size).min(b.len()))
                    .unwrap_or(&[])
                    .to_vec()
            })
            .unwrap_or_default();
        let step = self.next_step();
        let reply = self.send(
            worker,
            WorkerCommand::ReadAt {
                step,
                handle,
                offset,
                size,
            },
        )?;
        compare_bytes(&path, &expected, &reply.data)?;
        self.world.verified_files += 1;
        self.maybe_verify()
    }

    fn fresh_verify(&mut self, worker: usize, path: PathBuf) -> Result<(), RunError> {
        let step = self.next_step();
        let reply = self.send(
            worker,
            WorkerCommand::VerifyPath {
                step,
                path: path.clone(),
            },
        )?;
        self.world.verify_path(&path, &reply.data)
    }

    fn marker(
        &self,
        step: u64,
        worker: usize,
        handle: HandleId,
        path_id: usize,
        min_size: usize,
    ) -> Vec<u8> {
        let size = min_size
            .clamp(1, self.cfg.opsize_max.max(1))
            .min(self.cfg.flen.max(1));
        let marker = format!(
            "FSXMARK step={step} worker={worker} handle={handle} path={path_id} seed={} ",
            self.cfg.seed
        );
        let mut out = Vec::with_capacity(size);
        while out.len() < size {
            out.extend_from_slice(marker.as_bytes());
        }
        out.truncate(size);
        out
    }

    fn scenario_random(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            match self.rng.gen_range(0..6) {
                0 => self.scenario_shared_inode_once()?,
                1 => self.scenario_fd_reuse_once()?,
                2 => self.scenario_append_once()?,
                3 => self.scenario_unlink_recreate_once()?,
                4 => self.scenario_stronger_rights_once()?,
                _ => self.scenario_wordpress_like_once()?,
            }
        }
        Ok(())
    }

    fn scenario_shared_inode(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_shared_inode_once()?;
        }
        Ok(())
    }

    fn scenario_shared_inode_once(&mut self) -> Result<(), RunError> {
        let path = self.path(0);
        for worker in 1..=self.cfg.workers {
            self.open(worker, 1, path.clone(), OpenFlags::read_write_create())?;
        }
        for worker in 1..=self.cfg.workers {
            let step_hint = self.steps + 1;
            let data = self.marker(step_hint, worker, 1, 0, self.cfg.opsize_min.max(128));
            let offset = ((worker - 1) * data.len()) as u64;
            self.write_at(worker, 1, offset, data.clone())?;
            self.read_at_verify(worker, 1, offset, data.len())?;
        }
        self.fresh_verify(1, path)?;
        for worker in 1..=self.cfg.workers {
            self.close(worker, 1)?;
        }
        Ok(())
    }

    fn scenario_independent_offsets(&mut self) -> Result<(), RunError> {
        let path = self.path(0);
        for worker in 1..=self.cfg.workers {
            self.open(worker, 2, path.clone(), OpenFlags::read_write_create())?;
            let step_hint = self.steps + 1;
            let data = self.marker(step_hint, worker, 2, 0, 64);
            self.write_at(worker, 2, ((worker - 1) * 128) as u64, data)?;
            let step = self.next_step();
            self.send(
                worker,
                WorkerCommand::Seek {
                    step,
                    handle: 2,
                    offset: ((worker - 1) * 128) as u64,
                },
            )?;
            self.world.set_pos(worker, 2, ((worker - 1) * 128) as u64);
            self.read_at_verify(worker, 2, ((worker - 1) * 128) as u64, 64)?;
            self.close(worker, 2)?;
        }
        self.world.verify_all_fresh()
    }

    fn scenario_overlapping_writes(&mut self) -> Result<(), RunError> {
        let path = self.path(0);
        for worker in 1..=self.cfg.workers {
            self.open(worker, 3, path.clone(), OpenFlags::read_write_create())?;
            let data = self.marker(self.steps + 1, worker, 3, 0, 256);
            self.write_at(worker, 3, 0, data)?;
        }
        self.fresh_verify(1, path)?;
        for worker in 1..=self.cfg.workers {
            self.close(worker, 3)?;
        }
        Ok(())
    }

    fn scenario_stronger_rights(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_stronger_rights_once()?;
        }
        Ok(())
    }

    fn scenario_stronger_rights_once(&mut self) -> Result<(), RunError> {
        let path = self.path(1 % self.cfg.files);
        self.open(1, 10, path.clone(), OpenFlags::read_write_create())?;
        let data = self.marker(self.steps + 1, 1, 10, 1, 256);
        self.write_at(1, 10, 0, data.clone())?;
        self.close(1, 10)?;
        self.open(1, 11, path.clone(), OpenFlags::read_only())?;
        self.open(
            2.min(self.cfg.workers),
            12,
            path.clone(),
            OpenFlags::read_write_create(),
        )?;
        let other = self.marker(self.steps + 1, 2, 12, 1, 128);
        self.write_at(2.min(self.cfg.workers), 12, data.len() as u64, other)?;
        self.read_at_verify(1, 11, 0, data.len())?;
        self.fresh_verify(2.min(self.cfg.workers), path)?;
        self.close(1, 11)?;
        self.close(2.min(self.cfg.workers), 12)
    }

    fn scenario_fd_reuse(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_fd_reuse_once()?;
        }
        Ok(())
    }

    fn scenario_fd_reuse_once(&mut self) -> Result<(), RunError> {
        let a = self.cfg.root.join("a.log");
        let b = self.cfg.root.join("b.log");
        self.open(1, 20, a, OpenFlags::read_write_create())?;
        self.close(1, 20)?;
        self.open(
            2.min(self.cfg.workers),
            21,
            b.clone(),
            OpenFlags::read_write_create(),
        )?;
        let data = self.marker(self.steps + 1, 2, 21, 2, 128);
        self.write_at(2.min(self.cfg.workers), 21, 0, data)?;
        for i in 0..self.cfg.handles.min(16) {
            let h = 100 + i as u64;
            let p = self.cfg.root.join(format!("filler-{i}.tmp"));
            self.open(1, h, p, OpenFlags::read_write_create())?;
            self.close(1, h)?;
        }
        self.fresh_verify(1, b)?;
        self.close(2.min(self.cfg.workers), 21)
    }

    fn scenario_append(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_append_once()?;
        }
        Ok(())
    }

    fn scenario_append_once(&mut self) -> Result<(), RunError> {
        let path = self.cfg.root.join("append.log");
        for worker in 1..=self.cfg.workers {
            self.open(worker, 30, path.clone(), OpenFlags::append_create())?;
        }
        if self.cfg.parallelism > 1 && self.cfg.workers > 1 {
            let mut commands = Vec::new();
            let mut records = Vec::new();
            for worker in 1..=self.cfg.workers.min(self.cfg.parallelism) {
                let step = self.next_step();
                let data = self.marker(step, worker, 30, 3, 96);
                records.push((worker, data.clone()));
                commands.push((
                    worker,
                    WorkerCommand::WriteSeq {
                        step,
                        handle: 30,
                        data,
                    },
                ));
            }
            self.send_group(commands)?;
            let path_for_model = self.world.handle_path(1, 30)?;
            let original = self
                .world
                .files
                .entry(path_for_model.clone())
                .or_default()
                .clone();
            let actual = fs::read(&path_for_model).map_err(|e| {
                RunError::new("missing-file", format!("failed to read append file: {e}"))
            })?;
            let mut accepted = Vec::new();
            for (_, data) in &records {
                accepted.extend_from_slice(data);
            }
            let mut alt = Vec::new();
            for (_, data) in records.iter().rev() {
                alt.extend_from_slice(data);
            }
            if actual != [original.clone(), accepted.clone()].concat()
                && actual != [original.clone(), alt.clone()].concat()
            {
                return Err(RunError::new(
                    "wrong-bytes",
                    "parallel append records overlapped or were truncated",
                ));
            }
            let modeled = actual;
            self.world.files.insert(path_for_model, modeled);
            self.world.verified_files += 1;
        } else {
            for worker in 1..=self.cfg.workers {
                let data = self.marker(self.steps + 1, worker, 30, 3, 96);
                let step = self.next_step();
                self.send(
                    worker,
                    WorkerCommand::WriteSeq {
                        step,
                        handle: 30,
                        data: data.clone(),
                    },
                )?;
                self.world.append(worker, 30, &data)?;
            }
        }
        self.fresh_verify(1, path)?;
        for worker in 1..=self.cfg.workers {
            self.close(worker, 30)?;
        }
        Ok(())
    }

    fn scenario_unlink_recreate(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_unlink_recreate_once()?;
        }
        Ok(())
    }

    fn scenario_unlink_recreate_once(&mut self) -> Result<(), RunError> {
        let path = self.cfg.root.join("target.txt");
        self.open(1, 40, path.clone(), OpenFlags::read_write_create())?;
        self.write_at(1, 40, 0, self.marker(self.steps + 1, 1, 40, 4, 96))?;
        let step = self.next_step();
        self.send(
            2.min(self.cfg.workers),
            WorkerCommand::Unlink {
                step,
                path: path.clone(),
            },
        )?;
        self.world.unlink(&path);
        self.open(
            2.min(self.cfg.workers),
            41,
            path.clone(),
            OpenFlags::read_write_truncate(),
        )?;
        let new_data = self.marker(self.steps + 1, 2, 41, 4, 96);
        self.write_at(2.min(self.cfg.workers), 41, 0, new_data)?;
        self.fresh_verify(1, path)?;
        self.close(1, 40)?;
        self.close(2.min(self.cfg.workers), 41)
    }

    fn scenario_rename(&mut self) -> Result<(), RunError> {
        let a = self.cfg.root.join("rename-a.txt");
        let b = self.cfg.root.join("rename-b.txt");
        self.open(1, 50, a.clone(), OpenFlags::read_write_create())?;
        let data = self.marker(self.steps + 1, 1, 50, 5, 128);
        self.write_at(1, 50, 0, data)?;
        let step = self.next_step();
        self.send(
            2.min(self.cfg.workers),
            WorkerCommand::Rename {
                step,
                from: a.clone(),
                to: b.clone(),
            },
        )?;
        self.world.rename(&a, &b);
        self.fresh_verify(1, b)?;
        self.close(1, 50)
    }

    fn scenario_wordpress_like(&mut self) -> Result<(), RunError> {
        for _ in 0..self.cfg.numops {
            self.scenario_wordpress_like_once()?;
        }
        Ok(())
    }

    fn scenario_wordpress_like_once(&mut self) -> Result<(), RunError> {
        let plugin = self.cfg.root.join("wp-content/plugins/plugin.php");
        let theme = self.cfg.root.join("wp-content/themes/theme.php");
        let log = self.cfg.root.join("wp-content/debug.log");
        self.open(1, 60, plugin.clone(), OpenFlags::read_write_create())?;
        self.write_at(1, 60, 0, self.marker(self.steps + 1, 1, 60, 6, 128))?;
        self.close(1, 60)?;
        self.open(1, 61, plugin.clone(), OpenFlags::read_only())?;
        self.open(
            2.min(self.cfg.workers),
            62,
            theme.clone(),
            OpenFlags::read_write_create(),
        )?;
        self.write_at(
            2.min(self.cfg.workers),
            62,
            0,
            self.marker(self.steps + 1, 2, 62, 7, 128),
        )?;
        self.open(1, 63, log.clone(), OpenFlags::append_create())?;
        let step = self.next_step();
        self.send(
            1,
            WorkerCommand::SwitchLogSink {
                step,
                sink: LogSink::Handle(1, 63),
            },
        )?;
        let log_data = self.marker(self.steps + 1, 1, 63, 8, 96);
        let step = self.next_step();
        self.send(
            1,
            WorkerCommand::WriteLog {
                step,
                data: log_data.clone(),
            },
        )?;
        self.world.append(1, 63, &log_data)?;
        let step = self.next_step();
        self.send(
            2.min(self.cfg.workers),
            WorkerCommand::WriteStderr {
                step,
                data: b"stderr marker\n".to_vec(),
            },
        )?;
        self.fresh_verify(2.min(self.cfg.workers), plugin)?;
        self.fresh_verify(1, theme)?;
        self.fresh_verify(1, log)?;
        self.close(1, 61)?;
        self.close(2.min(self.cfg.workers), 62)?;
        self.close(1, 63)
    }

    fn scenario_injection(&mut self) -> Result<(), RunError> {
        let old = self.cfg.inject;
        if self.cfg.inject.is_none() {
            self.cfg.inject = Some(self.steps + self.cfg.workers as u64 + 1);
        }
        let r = self.scenario_shared_inode_once();
        self.cfg.inject = old;
        r
    }
}

fn compare_bytes(path: &Path, expected: &[u8], actual: &[u8]) -> Result<(), RunError> {
    if expected.len() != actual.len() {
        return Err(RunError::new(
            "wrong-size",
            format!(
                "{} expected {} bytes, got {} bytes",
                path.display(),
                expected.len(),
                actual.len()
            ),
        ));
    }
    if expected != actual {
        let offset = expected
            .iter()
            .zip(actual.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(0);
        return Err(RunError::new(
            "wrong-bytes",
            format!(
                "{} mismatch at offset {offset}: expected {:#04x}, got {:#04x}",
                path.display(),
                expected[offset],
                actual[offset]
            ),
        ));
    }
    Ok(())
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, RunError> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    collect_files_inner(root, &mut out)?;
    Ok(out)
}

fn collect_files_inner(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), RunError> {
    for entry in fs::read_dir(path).map_err(|e| {
        RunError::new(
            "syscall-error",
            format!("failed to read directory {}: {e}", path.display()),
        )
    })? {
        let entry = entry.map_err(|e| RunError::new("syscall-error", e))?;
        let path = entry.path();
        let md = entry
            .metadata()
            .map_err(|e| RunError::new("syscall-error", e))?;
        if md.is_dir() {
            collect_files_inner(&path, out)?;
        } else if md.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

fn prepare_root(root: &Path) -> Result<(), RunError> {
    let root_str = root.as_os_str().to_string_lossy();
    if root_str == "/" || root_str == "/tmp" || root_str == "/data" || root_str == "/work" {
        return Err(RunError::new(
            "syscall-error",
            format!(
                "refusing to clean broad run root {}; pass a child directory instead",
                root.display()
            ),
        ));
    }
    if root.exists() {
        fs::remove_dir_all(root).map_err(|e| {
            RunError::new(
                "syscall-error",
                format!("failed to remove run root {}: {e}", root.display()),
            )
        })?;
    }
    fs::create_dir_all(root).map_err(|e| {
        RunError::new(
            "syscall-error",
            format!("failed to create run root {}: {e}", root.display()),
        )
    })
}

fn stable_hash(data: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut h);
    h.finish()
}

fn verify_manifest(cfg: &RunConfig) -> Result<(), RunError> {
    let Some(manifest) = &cfg.manifest else {
        return Err(RunError::new(
            "missing-file",
            "verify-manifest requires --manifest",
        ));
    };
    let text = fs::read_to_string(manifest).map_err(|e| {
        RunError::new(
            "missing-file",
            format!("failed to read manifest {}: {e}", manifest.display()),
        )
    })?;
    for (lineno, line) in text.lines().enumerate() {
        let parts = line.split('\t').collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(RunError::new(
                "wrong-bytes",
                format!("invalid manifest line {}", lineno + 1),
            ));
        }
        let path = cfg.root.join(parts[0]);
        let expected_len = parts[1]
            .parse::<usize>()
            .map_err(|e| RunError::new("wrong-bytes", format!("invalid manifest length: {e}")))?;
        let expected_hash = u64::from_str_radix(parts[2], 16)
            .map_err(|e| RunError::new("wrong-bytes", format!("invalid manifest hash: {e}")))?;
        let actual = fs::read(&path).map_err(|e| {
            RunError::new(
                "missing-file",
                format!("failed to read {}: {e}", path.display()),
            )
        })?;
        if actual.len() != expected_len || stable_hash(&actual) != expected_hash {
            return Err(RunError::new(
                "wrong-bytes",
                format!("manifest mismatch for {}", path.display()),
            ));
        }
    }
    Ok(())
}

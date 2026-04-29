use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::Cli;

const FD_SLOTS: usize = 2;
const WORKERS: usize = 2;
const FILES: &[FileId] = &[FileId::A, FileId::B, FileId::C];
const SNAPSHOT_FILES: &[FileId] = &[FileId::A, FileId::B];
const REPORT_MAGIC: &[u8; 8] = b"FSXORB1\0";
const REPORT_VERSION: u32 = 2;
const STDERR_MARKER: &[u8; 77] =
    b"FSX_ORACLE_STDERR_MARKER_77_BYTES_DO_NOT_BELONG_IN_DATA_FILES_1234567890!!!!!";
const MAX_REPORT_MISMATCHES: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileId {
    A,
    B,
    C,
}

impl FileId {
    fn name(self) -> &'static str {
        match self {
            FileId::A => "A",
            FileId::B => "B",
            FileId::C => "C",
        }
    }

    fn path(self, root: &Path) -> PathBuf {
        match self {
            FileId::A | FileId::B => root.join(self.name()),
            FileId::C => tmp_file_path(root),
        }
    }

    fn tag(self) -> u8 {
        match self {
            FileId::A => 0,
            FileId::B => 1,
            FileId::C => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(FileId::A),
            1 => Ok(FileId::B),
            2 => Ok(FileId::C),
            _ => Err(format!("invalid file id tag {tag}")),
        }
    }

    fn snapshotted(self) -> bool {
        SNAPSHOT_FILES.contains(&self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenMode {
    ReadWriteCreate,
    AppendCreate,
    ReadWriteTruncate,
    ReadWriteCreateNew,
}

impl OpenMode {
    fn name(self) -> &'static str {
        match self {
            OpenMode::ReadWriteCreate => "read_write_create",
            OpenMode::AppendCreate => "append_create",
            OpenMode::ReadWriteTruncate => "read_write_truncate",
            OpenMode::ReadWriteCreateNew => "read_write_create_new",
        }
    }

    fn tag(self) -> u8 {
        match self {
            OpenMode::ReadWriteCreate => 0,
            OpenMode::AppendCreate => 1,
            OpenMode::ReadWriteTruncate => 2,
            OpenMode::ReadWriteCreateNew => 3,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(OpenMode::ReadWriteCreate),
            1 => Ok(OpenMode::AppendCreate),
            2 => Ok(OpenMode::ReadWriteTruncate),
            3 => Ok(OpenMode::ReadWriteCreateNew),
            _ => Err(format!("invalid open mode tag {tag}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WritePayload {
    ZeroBytes,
    Bytes32,
    Bytes4K,
    Bytes32K,
}

impl WritePayload {
    fn name(self) -> &'static str {
        match self {
            WritePayload::ZeroBytes => "zero_bytes",
            WritePayload::Bytes32 => "32_bytes",
            WritePayload::Bytes4K => "4_kb",
            WritePayload::Bytes32K => "32_kb",
        }
    }

    fn len(self) -> usize {
        match self {
            WritePayload::ZeroBytes => 0,
            WritePayload::Bytes32 => 32,
            WritePayload::Bytes4K => 4 * 1024,
            WritePayload::Bytes32K => 32 * 1024,
        }
    }

    fn tag(self) -> u8 {
        match self {
            WritePayload::ZeroBytes => 0,
            WritePayload::Bytes32 => 1,
            WritePayload::Bytes4K => 2,
            WritePayload::Bytes32K => 3,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(WritePayload::ZeroBytes),
            1 => Ok(WritePayload::Bytes32),
            2 => Ok(WritePayload::Bytes4K),
            3 => Ok(WritePayload::Bytes32K),
            _ => Err(format!("invalid write payload tag {tag}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadSize {
    ZeroBytes,
    Bytes32,
    Bytes4K,
}

impl ReadSize {
    fn name(self) -> &'static str {
        match self {
            ReadSize::ZeroBytes => "zero_bytes",
            ReadSize::Bytes32 => "32_bytes",
            ReadSize::Bytes4K => "4_kb",
        }
    }

    fn len(self) -> usize {
        match self {
            ReadSize::ZeroBytes => 0,
            ReadSize::Bytes32 => 32,
            ReadSize::Bytes4K => 4 * 1024,
        }
    }

    fn tag(self) -> u8 {
        match self {
            ReadSize::ZeroBytes => 0,
            ReadSize::Bytes32 => 1,
            ReadSize::Bytes4K => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(ReadSize::ZeroBytes),
            1 => Ok(ReadSize::Bytes32),
            2 => Ok(ReadSize::Bytes4K),
            _ => Err(format!("invalid read size tag {tag}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SeekTarget {
    Start,
    End,
}

impl SeekTarget {
    fn name(self) -> &'static str {
        match self {
            SeekTarget::Start => "start",
            SeekTarget::End => "end",
        }
    }

    fn tag(self) -> u8 {
        match self {
            SeekTarget::Start => 0,
            SeekTarget::End => 1,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(SeekTarget::Start),
            1 => Ok(SeekTarget::End),
            _ => Err(format!("invalid seek target tag {tag}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpSpec {
    Open(OpenMode),
    Close,
    Write(WritePayload),
    Read(ReadSize),
    Seek(SeekTarget),
    Fstat,
    Stat,
    ReadDir,
    WriteStderr,
    Delete,
}

impl OpSpec {
    fn needs_file(self) -> bool {
        matches!(self, OpSpec::Open(_) | OpSpec::Delete | OpSpec::Stat)
    }

    fn needs_fd(self) -> bool {
        !matches!(
            self,
            OpSpec::WriteStderr | OpSpec::Delete | OpSpec::Stat | OpSpec::ReadDir
        )
    }
}

fn op_catalog() -> &'static [OpSpec] {
    &[
        OpSpec::Open(OpenMode::ReadWriteCreate),
        OpSpec::Open(OpenMode::AppendCreate),
        OpSpec::Open(OpenMode::ReadWriteTruncate),
        OpSpec::Open(OpenMode::ReadWriteCreateNew),
        OpSpec::Close,
        OpSpec::Write(WritePayload::Bytes32),
        OpSpec::Read(ReadSize::Bytes32),
        OpSpec::Fstat,
        OpSpec::Stat,
        OpSpec::ReadDir,
        OpSpec::WriteStderr,
        OpSpec::Delete,
        // Unlikely
        // OpSpec::Write(WritePayload::ZeroBytes),
        // OpSpec::Write(WritePayload::Bytes4K),
        // OpSpec::Write(WritePayload::Bytes32K),
        // OpSpec::Read(ReadSize::ZeroBytes),
        // OpSpec::Read(ReadSize::Bytes4K),
        // OpSpec::Seek(SeekTarget::Start),
        // OpSpec::Seek(SeekTarget::End),
    ]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Open {
        slot: usize,
        file: FileId,
        mode: OpenMode,
    },
    Close {
        slot: usize,
    },
    Write {
        slot: usize,
        payload: WritePayload,
    },
    Read {
        slot: usize,
        size: ReadSize,
    },
    Seek {
        slot: usize,
        target: SeekTarget,
    },
    Fstat {
        slot: usize,
    },
    Stat {
        file: FileId,
    },
    ReadDir,
    WriteStderr,
    Delete {
        file: FileId,
    },
}

impl Op {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<(), String> {
        match self {
            Op::Open { slot, file, mode } => {
                write_u8(w, 0)?;
                write_u32(w, *slot as u32)?;
                write_u8(w, file.tag())?;
                write_u8(w, mode.tag())
            }
            Op::Close { slot } => {
                write_u8(w, 1)?;
                write_u32(w, *slot as u32)
            }
            Op::Write { slot, payload } => {
                write_u8(w, 2)?;
                write_u32(w, *slot as u32)?;
                write_u8(w, payload.tag())
            }
            Op::Read { slot, size } => {
                write_u8(w, 3)?;
                write_u32(w, *slot as u32)?;
                write_u8(w, size.tag())
            }
            Op::WriteStderr => write_u8(w, 4),
            Op::Delete { file } => {
                write_u8(w, 5)?;
                write_u8(w, file.tag())
            }
            Op::Seek { slot, target } => {
                write_u8(w, 6)?;
                write_u32(w, *slot as u32)?;
                write_u8(w, target.tag())
            }
            Op::Fstat { slot } => {
                write_u8(w, 7)?;
                write_u32(w, *slot as u32)
            }
            Op::Stat { file } => {
                write_u8(w, 8)?;
                write_u8(w, file.tag())
            }
            Op::ReadDir => write_u8(w, 9),
        }
    }

    fn read_from<R: Read>(r: &mut R) -> Result<Self, String> {
        match read_u8(r)? {
            0 => Ok(Op::Open {
                slot: read_u32(r)? as usize,
                file: FileId::from_tag(read_u8(r)?)?,
                mode: OpenMode::from_tag(read_u8(r)?)?,
            }),
            1 => Ok(Op::Close {
                slot: read_u32(r)? as usize,
            }),
            2 => Ok(Op::Write {
                slot: read_u32(r)? as usize,
                payload: WritePayload::from_tag(read_u8(r)?)?,
            }),
            3 => Ok(Op::Read {
                slot: read_u32(r)? as usize,
                size: ReadSize::from_tag(read_u8(r)?)?,
            }),
            4 => Ok(Op::WriteStderr),
            5 => Ok(Op::Delete {
                file: FileId::from_tag(read_u8(r)?)?,
            }),
            6 => Ok(Op::Seek {
                slot: read_u32(r)? as usize,
                target: SeekTarget::from_tag(read_u8(r)?)?,
            }),
            7 => Ok(Op::Fstat {
                slot: read_u32(r)? as usize,
            }),
            8 => Ok(Op::Stat {
                file: FileId::from_tag(read_u8(r)?)?,
            }),
            9 => Ok(Op::ReadDir),
            tag => Err(format!("invalid op tag {tag}")),
        }
    }

    fn pseudocode(self) -> String {
        match self {
            Op::Open { slot, file, mode } => {
                format!("h{} = open({}, {})", slot + 1, file.name(), mode.name())
            }
            Op::Close { slot } => format!("close(h{})", slot + 1),
            Op::Write { slot, payload } => format!("write(h{}, {})", slot + 1, payload.name()),
            Op::Read { slot, size } => format!("read(h{}, {})", slot + 1, size.name()),
            Op::WriteStderr => "write_stderr(77-byte-marker)".to_owned(),
            Op::Delete { file } => format!("delete({})", file.name()),
            Op::Seek { slot, target } => format!("seek(h{}, {})", slot + 1, target.name()),
            Op::Fstat { slot } => format!("fstat(h{})", slot + 1),
            Op::Stat { file } => format!("stat({})", file.name()),
            Op::ReadDir => "read_dir(case_dir)".to_owned(),
        }
    }
}

#[derive(Default)]
struct State {
    workers: Vec<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    slots: Vec<Option<File>>,
    leaked: Vec<File>,
}

struct Job {
    op: Op,
    case_root: Arc<PathBuf>,
}

#[derive(Debug)]
struct OracleReport {
    chain_len: usize,
    run: String,
    records: Vec<ComparableRecord>,
    snapshots: Vec<FileSnapshot>,
}

#[derive(Debug)]
struct FileSnapshot {
    case_id: usize,
    file: FileId,
    rel: PathBuf,
    exists: bool,
    len: u64,
    hash: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ComparableRecord {
    Case {
        case_id: u64,
        mask: u64,
    },
    Step {
        step: u32,
        worker: u32,
        op: Op,
    },
    Result {
        step: u32,
        worker: u32,
        rc: i64,
        err: OracleErr,
        data: Vec<u8>,
    },
    Snapshot {
        case_id: u64,
        file: FileId,
        rel: PathBuf,
        exists: bool,
        len: u64,
        hash: u64,
    },
    End {
        cases: u64,
    },
}

#[derive(Clone, Debug)]
struct Reply {
    result: OpResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OpResult {
    rc: i64,
    err: OracleErr,
    data: Vec<u8>,
}

impl OpResult {
    fn ok(rc: i64) -> Self {
        OpResult {
            rc,
            err: OracleErr::None,
            data: Vec::new(),
        }
    }

    fn ok_data(rc: i64, data: Vec<u8>) -> Self {
        OpResult {
            rc,
            err: OracleErr::None,
            data,
        }
    }

    fn err(err: OracleErr) -> Self {
        OpResult {
            rc: -1,
            err,
            data: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum OracleErr {
    None = 0,
    NotFound = 1,
    PermissionDenied = 2,
    AlreadyExists = 3,
    InvalidInput = 4,
    InvalidData = 5,
    UnexpectedEof = 6,
    WriteZero = 7,
    Interrupted = 8,
    Unsupported = 9,
    BadFd = 10,
    Other = 255,
}

impl OracleErr {
    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(OracleErr::None),
            1 => Ok(OracleErr::NotFound),
            2 => Ok(OracleErr::PermissionDenied),
            3 => Ok(OracleErr::AlreadyExists),
            4 => Ok(OracleErr::InvalidInput),
            5 => Ok(OracleErr::InvalidData),
            6 => Ok(OracleErr::UnexpectedEof),
            7 => Ok(OracleErr::WriteZero),
            8 => Ok(OracleErr::Interrupted),
            9 => Ok(OracleErr::Unsupported),
            10 => Ok(OracleErr::BadFd),
            255 => Ok(OracleErr::Other),
            _ => Err(format!("invalid oracle error tag {tag}")),
        }
    }
}

enum WorkerMessage {
    Run(Job),
    Stop,
}

struct ChainGenerator {
    templates: Vec<Op>,
}

impl ChainGenerator {
    fn new() -> Self {
        ChainGenerator {
            templates: concrete_ops(),
        }
    }

    fn generate(&self, len: usize) -> Vec<Vec<Op>> {
        let mut chains = Vec::new();
        self.build_chains(len, &mut Vec::new(), &mut chains);
        chains
    }

    fn build_chains(&self, len: usize, current: &mut Vec<Op>, out: &mut Vec<Vec<Op>>) {
        if current.len() == len {
            out.push(current.clone());
            return;
        }
        for op in &self.templates {
            current.push(*op);
            self.build_chains(len, current, out);
            current.pop();
        }
    }
}

struct Scheduler {
    worker_count: usize,
}

impl Scheduler {
    fn new(worker_count: usize) -> Self {
        Scheduler { worker_count }
    }

    fn schedule_count(&self, chain_len: usize) -> usize {
        if chain_len == 0 {
            1
        } else {
            self.worker_count.pow((chain_len - 1) as u32)
        }
    }

    fn workers<'a>(&'a self, chain_len: usize, mask: usize) -> impl Iterator<Item = usize> + 'a {
        let mut remaining = mask;
        (0..chain_len).map(move |idx| {
            if idx == 0 {
                0
            } else {
                let worker = remaining % self.worker_count;
                remaining /= self.worker_count;
                worker
            }
        })
    }
}

struct WorkerPool {
    state: Arc<Mutex<State>>,
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    reply_rx: mpsc::Receiver<Reply>,
    handles: Vec<thread::JoinHandle<()>>,
}

struct Progress {
    total: usize,
    next_print: Instant,
}

impl Progress {
    fn new(total: usize) -> Self {
        let progress = Progress {
            total,
            next_print: Instant::now() + Duration::from_secs(1),
        };
        progress.print(0);
        progress
    }

    fn maybe_print(&mut self, index: usize) {
        if Instant::now() >= self.next_print {
            self.print(index);
            self.next_print = Instant::now() + Duration::from_secs(1);
        }
    }

    fn finish(&self, index: usize) {
        self.print(index);
    }

    fn print(&self, index: usize) {
        let percent = if self.total == 0 {
            100.0
        } else {
            (index as f64 * 100.0) / self.total as f64
        };
        println!(
            "[{}] {:06.2}% {}/{}",
            current_timestamp(),
            percent,
            index,
            self.total
        );
    }
}

struct ReportWriter {
    writer: BufWriter<File>,
}

impl ReportWriter {
    fn create(path: &Path, chain_len: usize, run: &str) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
        // Deliberately avoid O_APPEND here. Wasmer issue #6525 affects
        // concurrent append-mode writes, while oracle reports are written
        // sequentially by the coordinator through this single handle.
        let file =
            File::create(path).map_err(|e| format!("failed to create {}: {e}", path.display()))?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(REPORT_MAGIC)
            .map_err(|e| format!("failed to write report header: {e}"))?;
        write_u32(&mut writer, REPORT_VERSION)?;
        write_u64(&mut writer, chain_len as u64)?;
        write_string(&mut writer, run)?;
        Ok(ReportWriter { writer })
    }

    fn case(&mut self, case_id: usize, mask: usize) -> Result<(), String> {
        write_u8(&mut self.writer, 1)?;
        write_u64(&mut self.writer, case_id as u64)?;
        write_u64(&mut self.writer, mask as u64)
    }

    fn step(&mut self, step: usize, worker: usize, op: Op) -> Result<(), String> {
        write_u8(&mut self.writer, 2)?;
        write_u32(&mut self.writer, step as u32)?;
        write_u32(&mut self.writer, worker as u32)?;
        op.write_to(&mut self.writer)
    }

    fn result(&mut self, step: usize, worker: usize, result: &OpResult) -> Result<(), String> {
        write_u8(&mut self.writer, 3)?;
        write_u32(&mut self.writer, step as u32)?;
        write_u32(&mut self.writer, worker as u32)?;
        write_i64(&mut self.writer, result.rc)?;
        write_u8(&mut self.writer, result.err as u8)?;
        write_bytes(&mut self.writer, &result.data)
    }

    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String> {
        write_u8(&mut self.writer, 4)?;
        write_u64(&mut self.writer, snapshot.case_id as u64)?;
        write_u8(&mut self.writer, snapshot.file.tag())?;
        write_string(&mut self.writer, &snapshot.rel.to_string_lossy())?;
        write_u8(&mut self.writer, u8::from(snapshot.exists))?;
        write_u64(&mut self.writer, snapshot.len)?;
        write_u64(&mut self.writer, snapshot.hash)
    }

    fn end(&mut self, cases: usize) -> Result<(), String> {
        write_u8(&mut self.writer, 5)?;
        write_u64(&mut self.writer, cases as u64)?;
        self.writer
            .flush()
            .map_err(|e| format!("failed to flush oracle report: {e}"))
    }
}

impl WorkerPool {
    fn start(worker_count: usize) -> Self {
        let state = Arc::new(Mutex::new(State {
            workers: empty_workers(worker_count),
        }));
        let (reply_tx, reply_rx) = mpsc::channel::<Reply>();
        let mut senders = Vec::new();
        let mut handles = Vec::new();

        for worker in 0..worker_count {
            let (tx, rx) = mpsc::channel::<WorkerMessage>();
            let replies = reply_tx.clone();
            let state = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                while let Ok(message) = rx.recv() {
                    match message {
                        WorkerMessage::Run(job) => {
                            let line = execute_job(worker, &state, job);
                            let _ = replies.send(line);
                        }
                        WorkerMessage::Stop => break,
                    }
                }
            }));
            senders.push(tx);
        }

        WorkerPool {
            state,
            senders,
            reply_rx,
            handles,
        }
    }

    fn reset_state(&self) {
        let mut state = self.state.lock().unwrap();
        let worker_count = state.workers.len();
        state.workers = empty_workers(worker_count);
    }

    fn run_case(
        &self,
        case_root: Arc<PathBuf>,
        chain: &[Op],
        scheduler: &Scheduler,
        mask: usize,
        report: &mut ReportWriter,
    ) -> Result<(), String> {
        for (idx, (op, worker)) in chain
            .iter()
            .copied()
            .zip(scheduler.workers(chain.len(), mask))
            .enumerate()
        {
            report.step(idx + 1, worker, op)?;
            self.senders[worker]
                .send(WorkerMessage::Run(Job {
                    op,
                    case_root: Arc::clone(&case_root),
                }))
                .map_err(|e| e.to_string())?;
            let reply = self.reply_rx.recv().map_err(|e| e.to_string())?;
            report.result(idx + 1, worker, &reply.result)?;
        }

        Ok(())
    }

    fn stop(self) {
        for tx in self.senders {
            let _ = tx.send(WorkerMessage::Stop);
        }
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

pub(crate) fn run(cli: Cli) -> Result<(), String> {
    let root = cli
        .fname
        .clone()
        .ok_or_else(|| "oracle root is required".to_owned())?;
    prepare_root(&root)?;
    let work_root = allocate_run_root(&root)?;
    let len = cli.numops.unwrap_or(2) as usize;
    if len == 0 {
        return Err("oracle sequence length must be greater than zero".to_owned());
    }

    let output = cli
        .oracle_output
        .clone()
        .unwrap_or_else(|| root.join("oracle-report.bin"));
    run_suite(&work_root, len, &output)?;

    if let Some(path) = &cli.oracle_expected {
        compare_reports(path, &output)?;
    } else if cli.oracle_output.is_none() {
        println!("oracle report written to {}", output.display());
    }

    Ok(())
}

pub(crate) fn verify_files(native_report: &Path, wasix_report: &Path) -> Result<(), String> {
    compare_reports(native_report, wasix_report)?;

    let native = read_report(native_report)?;
    let wasix = read_report(wasix_report)?;
    if native.snapshots.len() != wasix.snapshots.len() {
        return Err(format!(
            "report snapshot count mismatch: native={}, wasix={}",
            native.snapshots.len(),
            wasix.snapshots.len()
        ));
    }

    let wasix_base = wasix_report.parent().ok_or_else(|| {
        format!(
            "cannot infer external file root from report path {}",
            wasix_report.display()
        )
    })?;

    let mut checked = 0usize;
    for expected in &native.snapshots {
        let host_path = join_report_path(wasix_base, &wasix.run, &expected.rel)?;
        verify_snapshot_file(expected, &host_path)?;
        checked += 1;
    }

    println!("oracle external file verification ok: checked {checked} file snapshots");
    Ok(())
}

fn run_suite(root: &Path, len: usize, output: &Path) -> Result<(), String> {
    let generator = ChainGenerator::new();
    let chains = generator.generate(len);
    let scheduler = Scheduler::new(WORKERS);
    let total_cases = chains
        .iter()
        .map(|chain| scheduler.schedule_count(chain.len()))
        .sum::<usize>();

    let run = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid oracle run root {}", root.display()))?;
    let mut report = ReportWriter::create(output, len, run)?;

    let runner = WorkerPool::start(WORKERS);
    let mut progress = Progress::new(total_cases);

    let mut case_id = 0usize;
    for chain in chains {
        for mask in 0usize..scheduler.schedule_count(chain.len()) {
            case_id += 1;
            let case_name = format!("case-{case_id:06}");
            let case_root = root.join(&case_name);
            fs::create_dir_all(&case_root)
                .map_err(|e| format!("failed to create {}: {e}", case_root.display()))?;
            report.case(case_id, mask)?;
            runner.reset_state();
            runner.run_case(
                Arc::new(case_root.clone()),
                &chain,
                &scheduler,
                mask,
                &mut report,
            )?;
            snapshot(&case_root, case_id, &case_name, &mut report)?;
            runner.reset_state();
            cleanup_tmp_files(&case_root)?;
            progress.maybe_print(case_id);
        }
    }

    runner.stop();
    progress.finish(case_id);

    report.end(case_id)
}

fn execute_job(worker: usize, state: &Arc<Mutex<State>>, job: Job) -> Reply {
    let result = match job.op {
        Op::Open { slot, file, mode } => {
            let path = file.path(&job.case_root);
            let result = open_file(&path, mode);
            match result {
                Ok(file) => {
                    let mut state = state.lock().unwrap();
                    let worker_state = &mut state.workers[worker];
                    if let Some(old) = worker_state.slots[slot].take() {
                        // Mirrors `fd_slot = open(...)`: the old numeric fd leaks.
                        worker_state.leaked.push(old);
                    }
                    worker_state.slots[slot] = Some(file);
                    OpResult::ok(-1)
                }
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Close { slot } => {
            let mut state = state.lock().unwrap();
            if state.workers[worker].slots[slot].take().is_some() {
                OpResult::ok(-1)
            } else {
                OpResult::err(OracleErr::BadFd)
            }
        }
        Op::Write { slot, payload } => {
            let mut state = state.lock().unwrap();
            let Some(file) = state.workers[worker].slots[slot].as_mut() else {
                return Reply {
                    result: OpResult::err(OracleErr::BadFd),
                };
            };
            let buf = write_payload(slot, payload);
            match file.write(&buf) {
                Ok(n) => OpResult::ok(n as i64),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Read { slot, size } => {
            let mut state = state.lock().unwrap();
            let Some(file) = state.workers[worker].slots[slot].as_mut() else {
                return Reply {
                    result: OpResult::err(OracleErr::BadFd),
                };
            };
            let mut buf = vec![0u8; size.len()];
            match file.read(&mut buf) {
                Ok(n) => OpResult::ok_data(n as i64, buf[..n].to_vec()),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::WriteStderr => match std::io::stderr().write_all(STDERR_MARKER) {
            Ok(()) => OpResult::ok(STDERR_MARKER.len() as i64),
            Err(e) => OpResult::err(errcode(&e)),
        },
        Op::Delete { file } => {
            let path = file.path(&job.case_root);
            match fs::remove_file(&path) {
                Ok(()) => OpResult::ok(-1),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Seek { slot, target } => {
            let mut state = state.lock().unwrap();
            let Some(file) = state.workers[worker].slots[slot].as_mut() else {
                return Reply {
                    result: OpResult::err(OracleErr::BadFd),
                };
            };
            let seek_from = match target {
                SeekTarget::Start => SeekFrom::Start(0),
                SeekTarget::End => SeekFrom::End(0),
            };
            match file.seek(seek_from) {
                Ok(pos) => OpResult::ok(pos as i64),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Fstat { slot } => {
            let state = state.lock().unwrap();
            let Some(file) = state.workers[worker].slots[slot].as_ref() else {
                return Reply {
                    result: OpResult::err(OracleErr::BadFd),
                };
            };
            match file.metadata() {
                Ok(metadata) => OpResult::ok(metadata.len() as i64),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Stat { file } => {
            let path = file.path(&job.case_root);
            match fs::metadata(&path) {
                Ok(metadata) => OpResult::ok(metadata.len() as i64),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::ReadDir => match read_dir_listing(&job.case_root) {
            Ok(listing) => OpResult::ok_data(listing.len() as i64, listing),
            Err(e) => OpResult::err(errcode(&e)),
        },
    };

    Reply { result }
}

fn open_file(path: &Path, mode: OpenMode) -> std::io::Result<File> {
    match mode {
        OpenMode::ReadWriteCreate => OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path),
        OpenMode::AppendCreate => OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path),
        OpenMode::ReadWriteTruncate => OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path),
        OpenMode::ReadWriteCreateNew => OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path),
    }
}

fn concrete_ops() -> Vec<Op> {
    let mut ops = Vec::new();
    for spec in op_catalog() {
        if spec.needs_file() && !spec.needs_fd() {
            for file in FILES.iter().copied() {
                ops.push(expand_op(*spec, 0, Some(file)));
            }
        } else if !spec.needs_fd() {
            ops.push(expand_op(*spec, 0, None));
        } else {
            for slot in 0..FD_SLOTS {
                if spec.needs_file() {
                    for file in FILES.iter().copied() {
                        ops.push(expand_op(*spec, slot, Some(file)));
                    }
                } else {
                    ops.push(expand_op(*spec, slot, None));
                }
            }
        }
    }
    ops
}

fn expand_op(spec: OpSpec, slot: usize, file: Option<FileId>) -> Op {
    match spec {
        OpSpec::Open(mode) => Op::Open {
            slot,
            file: file.expect("open ops are expanded across files"),
            mode,
        },
        OpSpec::Close => Op::Close { slot },
        OpSpec::Write(payload) => Op::Write { slot, payload },
        OpSpec::Read(size) => Op::Read { slot, size },
        OpSpec::WriteStderr => Op::WriteStderr,
        OpSpec::Delete => Op::Delete {
            file: file.expect("delete ops are expanded across files"),
        },
        OpSpec::Seek(target) => Op::Seek { slot, target },
        OpSpec::Fstat => Op::Fstat { slot },
        OpSpec::Stat => Op::Stat {
            file: file.expect("stat ops are expanded across files"),
        },
        OpSpec::ReadDir => Op::ReadDir,
    }
}

fn write_payload(slot: usize, payload: WritePayload) -> Vec<u8> {
    let byte = if slot == 0 { b'A' } else { b'B' };
    vec![byte; payload.len()]
}

fn tmp_file_path(case_root: &Path) -> PathBuf {
    let case_name = case_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("case-unknown");
    let run_name = case_root
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("run-unknown");
    PathBuf::from(format!("/tmp/fsx-oracle-{run_name}-{case_name}-C"))
}

fn cleanup_tmp_files(case_root: &Path) -> Result<(), String> {
    for file in FILES.iter().copied().filter(|file| !file.snapshotted()) {
        let path = file.path(case_root);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("failed to remove {}: {e}", path.display())),
        }
    }
    Ok(())
}

fn read_dir_listing(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        entries.push(entry.file_name().to_string_lossy().into_owned());
    }
    entries.sort();
    Ok(entries.join("\n").into_bytes())
}

fn empty_slots() -> Vec<Option<File>> {
    (0..FD_SLOTS).map(|_| None).collect()
}

fn empty_workers(worker_count: usize) -> Vec<WorkerState> {
    (0..worker_count)
        .map(|_| WorkerState {
            slots: empty_slots(),
            leaked: Vec::new(),
        })
        .collect()
}

fn snapshot(
    root: &Path,
    case_id: usize,
    case_name: &str,
    report: &mut ReportWriter,
) -> Result<(), String> {
    for file in SNAPSHOT_FILES.iter().copied() {
        let path = file.path(root);
        let rel = format!("{case_name}/{}", file.name());
        match fs::read(&path) {
            Ok(data) => {
                report.snapshot(&FileSnapshot {
                    case_id,
                    file,
                    rel: PathBuf::from(rel),
                    exists: true,
                    len: data.len() as u64,
                    hash: stable_hash(&data),
                })?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                report.snapshot(&FileSnapshot {
                    case_id,
                    file,
                    rel: PathBuf::from(rel),
                    exists: false,
                    len: 0,
                    hash: 0,
                })?;
            }
            Err(e) => return Err(format!("failed to read {}: {e}", path.display())),
        }
    }
    Ok(())
}

fn compare_reports(expected_path: &Path, actual_path: &Path) -> Result<(), String> {
    let expected = read_report(expected_path)?;
    let actual = read_report(actual_path)?;
    if expected.chain_len != actual.chain_len {
        return Err(format!(
            "oracle chain length mismatch: expected {}, actual {}",
            expected.chain_len, actual.chain_len
        ));
    }
    if expected.records.len() != actual.records.len() {
        return Err(format!(
            "oracle record count mismatch: expected {}, actual {}",
            expected.records.len(),
            actual.records.len()
        ));
    }
    let mut mismatches = Vec::new();
    for (idx, (expected_record, actual_record)) in expected
        .records
        .iter()
        .zip(actual.records.iter())
        .enumerate()
    {
        if expected_record != actual_record {
            mismatches.push(format_mismatch(
                idx + 1,
                idx,
                &expected.records,
                expected_record,
                actual_record,
            ));
            if mismatches.len() >= MAX_REPORT_MISMATCHES {
                break;
            }
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "oracle found {} mismatch(es), showing up to {}\n\n{}",
            mismatches.len(),
            MAX_REPORT_MISMATCHES,
            mismatches.join("\n\n")
        ))
    }
}

fn format_mismatch(
    record_no: usize,
    record_idx: usize,
    records: &[ComparableRecord],
    expected: &ComparableRecord,
    actual: &ComparableRecord,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "mismatch at record {record_no}\nexpected: {expected:?}\nactual:   {actual:?}"
    ));
    if let Some(case_id) = record_case_id(expected)
        .or_else(|| record_case_id(actual))
        .or_else(|| current_case_id(records, record_idx))
    {
        out.push_str(&format!("\ncase {case_id} program:"));
        for line in case_program(records, case_id) {
            out.push('\n');
            out.push_str(&line);
        }
    }
    out
}

fn current_case_id(records: &[ComparableRecord], record_idx: usize) -> Option<u64> {
    records[..=record_idx].iter().rev().find_map(|record| {
        if let ComparableRecord::Case { case_id, .. } = record {
            Some(*case_id)
        } else {
            None
        }
    })
}

fn record_case_id(record: &ComparableRecord) -> Option<u64> {
    match record {
        ComparableRecord::Case { case_id, .. } | ComparableRecord::Snapshot { case_id, .. } => {
            Some(*case_id)
        }
        ComparableRecord::Step { .. }
        | ComparableRecord::Result { .. }
        | ComparableRecord::End { .. } => None,
    }
}

fn case_program(records: &[ComparableRecord], case_id: u64) -> Vec<String> {
    let mut in_case = false;
    let mut lines = Vec::new();
    for record in records {
        match record {
            ComparableRecord::Case { case_id: id, mask } if *id == case_id => {
                in_case = true;
                lines.push(format!("  mask={mask}"));
            }
            ComparableRecord::Case { .. } if in_case => break,
            ComparableRecord::Step { worker, op, .. } if in_case => {
                lines.push(format!("  T{}: {}", worker + 1, op.pseudocode()));
            }
            ComparableRecord::Result {
                worker,
                rc,
                err,
                data,
                ..
            } if in_case => {
                let data_suffix = if data.is_empty() {
                    String::new()
                } else {
                    format!(" data_len={}", data.len())
                };
                lines.push(format!(
                    "    -> T{} rc={} err={:?}{}",
                    worker + 1,
                    rc,
                    err,
                    data_suffix
                ));
            }
            _ => {}
        }
    }
    lines
}

fn read_report(path: &Path) -> Result<OracleReport, String> {
    let file = File::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut magic = [0u8; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|e| format!("failed to read {} header: {e}", path.display()))?;
    if &magic != REPORT_MAGIC {
        return Err(format!("{} is not an oracle binary report", path.display()));
    }
    let version = read_u32(&mut reader)?;
    if version != REPORT_VERSION {
        return Err(format!(
            "{} has unsupported oracle report version {}",
            path.display(),
            version
        ));
    }
    let chain_len = read_u64(&mut reader)? as usize;
    let run = read_string(&mut reader)?;
    let mut records = Vec::new();
    let mut snapshots = Vec::new();

    loop {
        let tag = match read_record_tag(&mut reader)? {
            Some(tag) => tag,
            None => break,
        };
        match tag {
            1 => records.push(ComparableRecord::Case {
                case_id: read_u64(&mut reader)?,
                mask: read_u64(&mut reader)?,
            }),
            2 => records.push(ComparableRecord::Step {
                step: read_u32(&mut reader)?,
                worker: read_u32(&mut reader)?,
                op: Op::read_from(&mut reader)?,
            }),
            3 => records.push(ComparableRecord::Result {
                step: read_u32(&mut reader)?,
                worker: read_u32(&mut reader)?,
                rc: read_i64(&mut reader)?,
                err: OracleErr::from_tag(read_u8(&mut reader)?)?,
                data: read_bytes(&mut reader)?,
            }),
            4 => {
                let case_id = read_u64(&mut reader)?;
                let file = FileId::from_tag(read_u8(&mut reader)?)?;
                let rel = PathBuf::from(read_string(&mut reader)?);
                let exists = read_u8(&mut reader)? != 0;
                let len = read_u64(&mut reader)?;
                let hash = read_u64(&mut reader)?;
                records.push(ComparableRecord::Snapshot {
                    case_id,
                    file,
                    rel: rel.clone(),
                    exists,
                    len,
                    hash,
                });
                snapshots.push(FileSnapshot {
                    case_id: case_id as usize,
                    file,
                    rel,
                    exists,
                    len,
                    hash,
                });
            }
            5 => records.push(ComparableRecord::End {
                cases: read_u64(&mut reader)?,
            }),
            _ => return Err(format!("{} has invalid record tag {}", path.display(), tag)),
        }
    }

    Ok(OracleReport {
        chain_len,
        run,
        records,
        snapshots,
    })
}

fn join_report_path(base: &Path, run: &str, rel: &Path) -> Result<PathBuf, String> {
    let run_path = Path::new(run);
    if run_path.is_absolute()
        || run_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("unsafe run path in report: {run}"));
    }
    if rel.is_absolute()
        || rel
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "unsafe relative file path in report: {}",
            rel.display()
        ));
    }
    Ok(base.join(run_path).join(rel))
}

fn verify_snapshot_file(expected: &FileSnapshot, host_path: &Path) -> Result<(), String> {
    if !expected.exists {
        if host_path.exists() {
            return Err(format!(
                "case {} file {} expected absent, but host file exists at {}",
                expected.case_id,
                expected.file.name(),
                host_path.display()
            ));
        }
        return Ok(());
    }

    let data = fs::read(host_path).map_err(|e| {
        format!(
            "case {} file {} expected present, failed to read {}: {e}",
            expected.case_id,
            expected.file.name(),
            host_path.display()
        )
    })?;
    let actual_len = data.len() as u64;
    let actual_hash = stable_hash(&data);

    if actual_len != expected.len {
        return Err(format!(
            "case {} file {} external len mismatch at {}: expected {}, actual {}",
            expected.case_id,
            expected.file.name(),
            host_path.display(),
            expected.len,
            actual_len
        ));
    }
    if actual_hash != expected.hash {
        return Err(format!(
            "case {} file {} external hash mismatch at {}: expected {:016x}, actual {:016x}",
            expected.case_id,
            expected.file.name(),
            host_path.display(),
            expected.hash,
            actual_hash
        ));
    }

    Ok(())
}

fn prepare_root(root: &Path) -> Result<(), String> {
    let root_str = root.as_os_str().to_string_lossy();
    if root_str == "/" || root_str == "/tmp" || root_str == "/data" || root_str == "/volume" {
        return Err(format!(
            "refusing to use broad oracle root {}; pass a child directory",
            root.display()
        ));
    }
    if root.exists() {
        if !root.is_dir() {
            return Err(format!("oracle root {} is not a directory", root.display()));
        }
    } else {
        fs::create_dir_all(root)
            .map_err(|e| format!("failed to create {}: {e}", root.display()))?;
    }
    Ok(())
}

fn allocate_run_root(root: &Path) -> Result<PathBuf, String> {
    for idx in 1..=1_000_000usize {
        let candidate = root.join(format!("run-{idx:06}"));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("failed to create {}: {e}", candidate.display())),
        }
    }
    Err(format!(
        "failed to allocate a fresh run directory under {}",
        root.display()
    ))
}

fn errcode(e: &std::io::Error) -> OracleErr {
    match e.kind() {
        std::io::ErrorKind::NotFound => OracleErr::NotFound,
        std::io::ErrorKind::PermissionDenied => OracleErr::PermissionDenied,
        std::io::ErrorKind::AlreadyExists => OracleErr::AlreadyExists,
        std::io::ErrorKind::InvalidInput => OracleErr::InvalidInput,
        std::io::ErrorKind::InvalidData => OracleErr::InvalidData,
        std::io::ErrorKind::UnexpectedEof => OracleErr::UnexpectedEof,
        std::io::ErrorKind::WriteZero => OracleErr::WriteZero,
        std::io::ErrorKind::Interrupted => OracleErr::Interrupted,
        std::io::ErrorKind::Unsupported => OracleErr::Unsupported,
        _ if e.raw_os_error() == Some(9) => OracleErr::BadFd,
        _ => OracleErr::Other,
    }
}

fn stable_hash(data: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for byte in data {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn current_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn write_u8<W: Write>(w: &mut W, value: u8) -> Result<(), String> {
    w.write_all(&[value])
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_u32<W: Write>(w: &mut W, value: u32) -> Result<(), String> {
    w.write_all(&value.to_le_bytes())
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_u64<W: Write>(w: &mut W, value: u64) -> Result<(), String> {
    w.write_all(&value.to_le_bytes())
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_i64<W: Write>(w: &mut W, value: i64) -> Result<(), String> {
    w.write_all(&value.to_le_bytes())
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> Result<(), String> {
    write_u32(w, bytes.len() as u32)?;
    w.write_all(bytes)
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_string<W: Write>(w: &mut W, value: &str) -> Result<(), String> {
    write_bytes(w, value.as_bytes())
}

fn read_record_tag<R: Read>(r: &mut R) -> Result<Option<u8>, String> {
    let mut buf = [0u8; 1];
    match r.read(&mut buf) {
        Ok(0) => Ok(None),
        Ok(1) => Ok(Some(buf[0])),
        Ok(_) => unreachable!(),
        Err(e) => Err(format!("failed to read oracle report: {e}")),
    }
}

fn read_u8<R: Read>(r: &mut R) -> Result<u8, String> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(buf[0])
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(u64::from_le_bytes(buf))
}

fn read_i64<R: Read>(r: &mut R) -> Result<i64, String> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(i64::from_le_bytes(buf))
}

fn read_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, String> {
    let len = read_u32(r)? as usize;
    let mut bytes = vec![0u8; len];
    r.read_exact(&mut bytes)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(bytes)
}

fn read_string<R: Read>(r: &mut R) -> Result<String, String> {
    let bytes = read_bytes(r)?;
    String::from_utf8(bytes).map_err(|e| format!("invalid utf-8 in oracle report: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn generated_scheduled_case_count(len: usize) -> usize {
        let generator = ChainGenerator::new();
        let scheduler = Scheduler::new(WORKERS);
        generator
            .generate(len)
            .iter()
            .map(|chain| scheduler.schedule_count(chain.len()))
            .sum()
    }

    fn canonicalize_workers(workers: &[usize]) -> Vec<usize> {
        let mut next = 0usize;
        let mut mapping = Vec::<Option<usize>>::new();
        let mut canonical = Vec::with_capacity(workers.len());
        for worker in workers {
            if mapping.len() <= *worker {
                mapping.resize(*worker + 1, None);
            }
            let mapped = match mapping[*worker] {
                Some(mapped) => mapped,
                None => {
                    let mapped = next;
                    next += 1;
                    mapping[*worker] = Some(mapped);
                    mapped
                }
            };
            canonical.push(mapped);
        }
        canonical
    }

    fn raw_worker_patterns(len: usize, worker_count: usize) -> BTreeSet<Vec<usize>> {
        let mut patterns = BTreeSet::new();
        for mask in 0..worker_count.pow(len as u32) {
            let mut remaining = mask;
            let mut workers = Vec::with_capacity(len);
            for _ in 0..len {
                workers.push(remaining % worker_count);
                remaining /= worker_count;
            }
            patterns.insert(canonicalize_workers(&workers));
        }
        patterns
    }

    fn scheduled_worker_patterns(len: usize, worker_count: usize) -> BTreeSet<Vec<usize>> {
        let scheduler = Scheduler::new(worker_count);
        let chain = vec![
            Op::Close { slot: 0 },
            Op::Close { slot: 0 },
            Op::Close { slot: 0 },
            Op::Close { slot: 0 },
        ];
        let chain = &chain[..len];
        let mut patterns = BTreeSet::new();
        for mask in 0..scheduler.schedule_count(chain.len()) {
            let workers = scheduler.workers(chain.len(), mask).collect::<Vec<_>>();
            patterns.insert(canonicalize_workers(&workers));
        }
        patterns
    }

    fn temp_oracle_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("fsx-oracle-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn generator_expands_all_operation_and_worker_permutations() {
        assert_eq!(FILES.len(), 3);
        assert_eq!(SNAPSHOT_FILES.len(), 2);
        assert_eq!(concrete_ops().len(), 40);
        assert_eq!(generated_scheduled_case_count(1), 40);
        assert_eq!(generated_scheduled_case_count(2), 3200);
        assert_eq!(generated_scheduled_case_count(3), 256000);
        assert_eq!(generated_scheduled_case_count(4), 20480000);
    }

    #[test]
    fn scheduler_keeps_one_representative_per_thread_renaming_class() {
        for len in 1..=4 {
            assert_eq!(
                scheduled_worker_patterns(len, WORKERS),
                raw_worker_patterns(len, WORKERS)
            );
        }
    }

    #[test]
    fn fd_slots_are_scoped_to_executing_worker() {
        let root = Arc::new(temp_oracle_dir("per-worker-fd"));
        let state = Arc::new(Mutex::new(State {
            workers: empty_workers(2),
        }));

        let t1_open = execute_job(
            0,
            &state,
            Job {
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
                case_root: Arc::clone(&root),
            },
        );
        let t2_open = execute_job(
            1,
            &state,
            Job {
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
                case_root: Arc::clone(&root),
            },
        );
        let t1_close = execute_job(
            0,
            &state,
            Job {
                op: Op::Close { slot: 0 },
                case_root: Arc::clone(&root),
            },
        );
        let t1_write_after_close = execute_job(
            0,
            &state,
            Job {
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
                case_root: Arc::clone(&root),
            },
        );
        let t2_write_after_t1_close = execute_job(
            1,
            &state,
            Job {
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
                case_root: Arc::clone(&root),
            },
        );

        assert_eq!(t1_open.result.err, OracleErr::None);
        assert_eq!(t2_open.result.err, OracleErr::None);
        assert_eq!(t1_close.result.err, OracleErr::None);
        assert_eq!(t1_write_after_close.result.err, OracleErr::BadFd);
        assert_eq!(t2_write_after_t1_close.result, OpResult::ok(32));

        let _ = fs::remove_dir_all(root.as_ref());
    }
}

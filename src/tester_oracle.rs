use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    mem::ManuallyDrop,
    os::fd::{AsRawFd, FromRawFd, RawFd},
    path::{Component, Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::Cli;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

const FD_SLOTS: usize = 2;
const WORKERS: usize = 2;
const FILES: &[FileId] = &[
    FileId::A,
    FileId::B,
    FileId::Fixture1,
    // FileId::C is the /tmp-backed file. Keep it dormant while we focus on volume files.
    // FileId::C,
];
const SNAPSHOT_FILES: &[FileId] = &[FileId::A, FileId::B];
const FIXTURE_FILES: &[FileId] = &[FileId::Fixture1, FileId::Fixture2];
const REPORT_MAGIC: &[u8; 8] = b"FSXORB2\0";
const REPORT_VERSION: u32 = 1;
const STDERR_MARKER: &[u8; 77] =
    b"FSX_ORACLE_STDERR_MARKER_77_BYTES_DO_NOT_BELONG_IN_DATA_FILES_1234567890!!!!!";
const MAX_REPORT_MISMATCHES: usize = 20;
const REQUIRE_MUTATION: bool = true;
const NORMALIZATION_VERSION: u32 = 4;
const ERROR_NORMALIZATION_VERSION: u32 = 1;
const THEORY_TRIAL_1_FIXTURE_COUNT: usize = 20_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum FileId {
    A,
    B,
    C,
    Fixture1,
    Fixture2,
}

impl FileId {
    fn name(self) -> &'static str {
        match self {
            FileId::A => "A",
            FileId::B => "B",
            FileId::C => "C",
            FileId::Fixture1 => "F1",
            FileId::Fixture2 => "F2",
        }
    }

    fn path(self, root: &Path) -> PathBuf {
        match self {
            FileId::A | FileId::B => root.join(self.name()),
            FileId::C => tmp_file_path(root),
            FileId::Fixture1 | FileId::Fixture2 => {
                panic!("fixture paths require a fixture root and case id")
            }
        }
    }

    fn resolved_path(self, root: &Path, fixture_root: &Path, fixture_id: usize) -> PathBuf {
        match self {
            FileId::A | FileId::B | FileId::C => self.path(root),
            FileId::Fixture1 | FileId::Fixture2 => fixture_path(fixture_root, fixture_id, self),
        }
    }

    fn tag(self) -> u8 {
        match self {
            FileId::A => 0,
            FileId::B => 1,
            FileId::C => 2,
            FileId::Fixture1 => 3,
            FileId::Fixture2 => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(FileId::A),
            1 => Ok(FileId::B),
            2 => Ok(FileId::C),
            3 => Ok(FileId::Fixture1),
            4 => Ok(FileId::Fixture2),
            _ => Err(format!("invalid file id tag {tag}")),
        }
    }

    fn snapshotted(self) -> bool {
        SNAPSHOT_FILES.contains(&self)
    }

    fn canonicalizable_index(self) -> Option<usize> {
        match self {
            FileId::A => Some(0),
            FileId::B => Some(1),
            FileId::C => None,
            FileId::Fixture1 => None,
            FileId::Fixture2 => None,
        }
    }

    fn is_fixture(self) -> bool {
        matches!(self, FileId::Fixture1 | FileId::Fixture2)
    }

    fn from_canonicalizable_index(index: usize) -> Self {
        match index {
            0 => FileId::A,
            1 => FileId::B,
            _ => panic!("invalid canonical file index {index}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenMode {
    ReadWriteCreate,
    AppendCreate,
    ReadWriteTruncate,
    ReadWriteCreateNew,
    ReadOnlyExisting,
}

impl OpenMode {
    fn is_mutation(self) -> bool {
        match self {
            OpenMode::ReadWriteCreate => true,
            OpenMode::AppendCreate => true,
            OpenMode::ReadWriteTruncate => true,
            OpenMode::ReadWriteCreateNew => true,
            OpenMode::ReadOnlyExisting => false,
        }
    }

    fn name(self) -> &'static str {
        match self {
            OpenMode::ReadWriteCreate => "read_write_create",
            OpenMode::AppendCreate => "append_create",
            OpenMode::ReadWriteTruncate => "read_write_truncate",
            OpenMode::ReadWriteCreateNew => "read_write_create_new",
            OpenMode::ReadOnlyExisting => "read_only_existing",
        }
    }

    fn tag(self) -> u8 {
        match self {
            OpenMode::ReadWriteCreate => 0,
            OpenMode::AppendCreate => 1,
            OpenMode::ReadWriteTruncate => 2,
            OpenMode::ReadWriteCreateNew => 3,
            OpenMode::ReadOnlyExisting => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0 => Ok(OpenMode::ReadWriteCreate),
            1 => Ok(OpenMode::AppendCreate),
            2 => Ok(OpenMode::ReadWriteTruncate),
            3 => Ok(OpenMode::ReadWriteCreateNew),
            4 => Ok(OpenMode::ReadOnlyExisting),
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

#[allow(dead_code)]
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
    Dup,
    Delete,
}

impl OpSpec {
    fn needs_file(self) -> bool {
        match self {
            OpSpec::Open(_) => true,
            OpSpec::Close => false,
            OpSpec::Write(_) => false,
            OpSpec::Read(_) => false,
            OpSpec::Seek(_) => false,
            OpSpec::Fstat => false,
            OpSpec::Stat => true,
            OpSpec::ReadDir => false,
            OpSpec::WriteStderr => false,
            OpSpec::Dup => false,
            OpSpec::Delete => true,
        }
    }

    fn needs_fd(self) -> bool {
        match self {
            OpSpec::Open(_) => true,
            OpSpec::Close => true,
            OpSpec::Write(_) => true,
            OpSpec::Read(_) => true,
            OpSpec::Seek(_) => true,
            OpSpec::Fstat => true,
            OpSpec::Stat => false,
            OpSpec::ReadDir => false,
            OpSpec::WriteStderr => false,
            OpSpec::Dup => true,
            OpSpec::Delete => false,
        }
    }

    fn name(self) -> String {
        match self {
            OpSpec::Open(mode) => format!("open({})", mode.name()),
            OpSpec::Close => "close".to_owned(),
            OpSpec::Write(payload) => format!("write({}:{})", payload.name(), payload.len()),
            OpSpec::Read(size) => format!("read({}:{})", size.name(), size.len()),
            OpSpec::Seek(target) => format!("seek({})", target.name()),
            OpSpec::Fstat => "fstat".to_owned(),
            OpSpec::Stat => "stat".to_owned(),
            OpSpec::ReadDir => "read_dir".to_owned(),
            OpSpec::WriteStderr => format!("write_stderr({})", STDERR_MARKER.len()),
            OpSpec::Dup => "dup".to_owned(),
            OpSpec::Delete => "delete".to_owned(),
        }
    }
}

fn op_catalog() -> &'static [OpSpec] {
    &[
        OpSpec::Close,
        OpSpec::Open(OpenMode::AppendCreate),
        OpSpec::Open(OpenMode::ReadWriteCreate),
        OpSpec::Read(ReadSize::Bytes32),
        OpSpec::Write(WritePayload::Bytes32),
        // Unlikely
        // OpSpec::Delete,
        // OpSpec::Dup,
        // OpSpec::Open(OpenMode::ReadOnlyExisting),
        // OpSpec::Open(OpenMode::ReadWriteCreateNew),
        // OpSpec::Open(OpenMode::ReadWriteTruncate),
        // OpSpec::Fstat,
        // OpSpec::ReadDir,
        // OpSpec::Read(ReadSize::Bytes4K),
        // OpSpec::Stat,
        // OpSpec::WriteStderr,
        // OpSpec::Write(WritePayload::ZeroBytes),
        // OpSpec::Write(WritePayload::Bytes4K),
        // OpSpec::Write(WritePayload::Bytes32K),
        // OpSpec::Read(ReadSize::ZeroBytes),
        // OpSpec::Seek(SeekTarget::Start),
        // OpSpec::Seek(SeekTarget::End),
        // OpSpec::Dup2,
        // OpSpec::FdRenumber,
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
    Dup {
        src_slot: usize,
        dst_slot: usize,
    },
    Delete {
        file: FileId,
    },
}

impl Op {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<(), String> {
        match self {
            Op::Open { slot, file, mode } => {
                write_u8(w, 0)?;
                write_u8(w, *slot as u8)?;
                write_u8(w, file.tag())?;
                write_u8(w, mode.tag())
            }
            Op::Close { slot } => {
                write_u8(w, 1)?;
                write_u8(w, *slot as u8)
            }
            Op::Write { slot, payload } => {
                write_u8(w, 2)?;
                write_u8(w, *slot as u8)?;
                write_u8(w, payload.tag())
            }
            Op::Read { slot, size } => {
                write_u8(w, 3)?;
                write_u8(w, *slot as u8)?;
                write_u8(w, size.tag())
            }
            Op::WriteStderr => write_u8(w, 4),
            Op::Delete { file } => {
                write_u8(w, 5)?;
                write_u8(w, file.tag())
            }
            Op::Seek { slot, target } => {
                write_u8(w, 6)?;
                write_u8(w, *slot as u8)?;
                write_u8(w, target.tag())
            }
            Op::Fstat { slot } => {
                write_u8(w, 7)?;
                write_u8(w, *slot as u8)
            }
            Op::Stat { file } => {
                write_u8(w, 8)?;
                write_u8(w, file.tag())
            }
            Op::ReadDir => write_u8(w, 9),
            Op::Dup { src_slot, dst_slot } => {
                write_u8(w, 10)?;
                write_u8(w, *src_slot as u8)?;
                write_u8(w, *dst_slot as u8)
            }
        }
    }

    fn read_from<R: Read>(r: &mut R) -> Result<Self, String> {
        match read_u8(r)? {
            0 => Ok(Op::Open {
                slot: read_u8(r)? as usize,
                file: FileId::from_tag(read_u8(r)?)?,
                mode: OpenMode::from_tag(read_u8(r)?)?,
            }),
            1 => Ok(Op::Close {
                slot: read_u8(r)? as usize,
            }),
            2 => Ok(Op::Write {
                slot: read_u8(r)? as usize,
                payload: WritePayload::from_tag(read_u8(r)?)?,
            }),
            3 => Ok(Op::Read {
                slot: read_u8(r)? as usize,
                size: ReadSize::from_tag(read_u8(r)?)?,
            }),
            4 => Ok(Op::WriteStderr),
            5 => Ok(Op::Delete {
                file: FileId::from_tag(read_u8(r)?)?,
            }),
            6 => Ok(Op::Seek {
                slot: read_u8(r)? as usize,
                target: SeekTarget::from_tag(read_u8(r)?)?,
            }),
            7 => Ok(Op::Fstat {
                slot: read_u8(r)? as usize,
            }),
            8 => Ok(Op::Stat {
                file: FileId::from_tag(read_u8(r)?)?,
            }),
            9 => Ok(Op::ReadDir),
            10 => Ok(Op::Dup {
                src_slot: read_u8(r)? as usize,
                dst_slot: read_u8(r)? as usize,
            }),
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
            Op::Dup { src_slot, dst_slot } => {
                format!("h{} = dup(h{})", dst_slot + 1, src_slot + 1)
            }
            Op::Delete { file } => format!("delete({})", file.name()),
            Op::Seek { slot, target } => format!("seek(h{}, {})", slot + 1, target.name()),
            Op::Fstat { slot } => format!("fstat(h{})", slot + 1),
            Op::Stat { file } => format!("stat({})", file.name()),
            Op::ReadDir => "read_dir(case_dir)".to_owned(),
        }
    }

    fn is_mutation(self) -> bool {
        match self {
            Op::Open { mode, .. } => mode.is_mutation(),
            Op::Close { .. } => false,
            Op::Write { .. } => true,
            Op::Read { .. } => false,
            Op::Seek { .. } => false,
            Op::Fstat { .. } => false,
            Op::Stat { .. } => false,
            Op::ReadDir => false,
            Op::WriteStderr => false,
            Op::Dup { .. } => true,
            Op::Delete { .. } => true,
        }
    }

    fn requires_fd(self) -> bool {
        match self {
            Op::Open { .. } => false,
            Op::Close { .. } => true,
            Op::Write { .. } => true,
            Op::Read { .. } => true,
            Op::Seek { .. } => true,
            Op::Fstat { .. } => true,
            Op::Stat { .. } => false,
            Op::ReadDir => false,
            Op::WriteStderr => false,
            Op::Dup { .. } => true,
            Op::Delete { .. } => false,
        }
    }

    fn slot(self) -> Option<usize> {
        match self {
            Op::Open { slot, .. } => Some(slot),
            Op::Close { slot } => Some(slot),
            Op::Write { slot, .. } => Some(slot),
            Op::Read { slot, .. } => Some(slot),
            Op::Seek { slot, .. } => Some(slot),
            Op::Fstat { slot } => Some(slot),
            Op::Stat { .. } => None,
            Op::ReadDir => None,
            Op::WriteStderr => None,
            Op::Dup { src_slot, .. } => Some(src_slot),
            Op::Delete { .. } => None,
        }
    }

    fn file(self) -> Option<FileId> {
        match self {
            Op::Open { file, .. } => Some(file),
            Op::Close { .. } => None,
            Op::Write { .. } => None,
            Op::Read { .. } => None,
            Op::Seek { .. } => None,
            Op::Fstat { .. } => None,
            Op::Stat { file } => Some(file),
            Op::ReadDir => None,
            Op::WriteStderr => None,
            Op::Dup { .. } => None,
            Op::Delete { file } => Some(file),
        }
    }

    fn with_slot(self, canonical_slot: usize) -> Self {
        match self {
            Op::Open { file, mode, .. } => Op::Open {
                slot: canonical_slot,
                file,
                mode,
            },
            Op::Close { .. } => Op::Close {
                slot: canonical_slot,
            },
            Op::Write { payload, .. } => Op::Write {
                slot: canonical_slot,
                payload,
            },
            Op::Read { size, .. } => Op::Read {
                slot: canonical_slot,
                size,
            },
            Op::Seek { target, .. } => Op::Seek {
                slot: canonical_slot,
                target,
            },
            Op::Fstat { .. } => Op::Fstat {
                slot: canonical_slot,
            },
            Op::Stat { .. } => self,
            Op::ReadDir => self,
            Op::WriteStderr => self,
            Op::Dup { dst_slot, .. } => Op::Dup {
                src_slot: canonical_slot,
                dst_slot,
            },
            Op::Delete { .. } => self,
        }
    }

    fn with_file(self, canonical_file: FileId) -> Self {
        match self {
            Op::Open { slot, mode, .. } => Op::Open {
                slot,
                file: canonical_file,
                mode,
            },
            Op::Stat { .. } => Op::Stat {
                file: canonical_file,
            },
            Op::Delete { .. } => Op::Delete {
                file: canonical_file,
            },
            Op::Close { .. } => self,
            Op::Write { .. } => self,
            Op::Read { .. } => self,
            Op::Seek { .. } => self,
            Op::Fstat { .. } => self,
            Op::ReadDir => self,
            Op::WriteStderr => self,
            Op::Dup { .. } => self,
        }
    }
}

#[derive(Default)]
struct State {
    workers: Vec<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    slots: Vec<FdSlot>,
    leaked: Vec<File>,
}

enum FdSlot {
    NeverOpened,
    Open(File),
    Closed(RawFd),
}

struct Job {
    op: Op,
    case_root: Arc<PathBuf>,
    fixture_id: usize,
    fixture_root: Arc<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScheduledStep {
    worker: usize,
    op: Op,
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
    Result {
        step: u32,
        worker: u32,
        op: Op,
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

    fn for_each<F>(&self, len: usize, mut f: F) -> Result<(), String>
    where
        F: FnMut(&[Op]) -> Result<bool, String>,
    {
        self.visit_chains(len, &mut Vec::new(), &mut f).map(|_| ())
    }

    fn chain_count(&self, len: usize) -> Result<usize, String> {
        self.templates
            .len()
            .checked_pow(len as u32)
            .ok_or_else(|| format!("oracle chain count overflows usize for length {len}"))
    }

    fn visit_chains<F>(&self, len: usize, current: &mut Vec<Op>, f: &mut F) -> Result<bool, String>
    where
        F: FnMut(&[Op]) -> Result<bool, String>,
    {
        if current.len() == len {
            return f(current);
        }
        for op in &self.templates {
            current.push(*op);
            let should_continue = self.visit_chains(len, current, f)?;
            current.pop();
            if !should_continue {
                return Ok(false);
            }
        }
        Ok(true)
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

pub(crate) struct OracleProgress {
    pub(crate) timestamp: String,
    pub(crate) percent: f64,
    pub(crate) index: usize,
    pub(crate) total: usize,
    pub(crate) line: String,
}

pub(crate) struct OracleStatus {
    pub(crate) phase: &'static str,
    pub(crate) message: String,
    pub(crate) expected: Option<String>,
    pub(crate) actual: Option<String>,
}

pub(crate) struct OracleVerifyProgress {
    pub(crate) timestamp: String,
    pub(crate) percent: f64,
    pub(crate) index: usize,
    pub(crate) total: usize,
    pub(crate) line: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OracleShard {
    start: usize,
    count: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OracleSelection {
    Exhaustive(OracleShard),
    Sample { count: usize, seed: u64 },
}

impl OracleShard {
    fn from_cli(cli: &Cli) -> Result<Self, String> {
        let start = cli.oracle_case_start.unwrap_or(1);
        if start == 0 {
            return Err("--case-start must be greater than zero".to_owned());
        }
        if cli.oracle_case_count == Some(0) {
            return Err("--case-count must be greater than zero".to_owned());
        }
        Ok(OracleShard {
            start,
            count: cli.oracle_case_count,
        })
    }

    fn end_exclusive(self, total_cases: usize) -> Result<usize, String> {
        if self.start > total_cases {
            return Err(format!(
                "--case-start {} is past final case {}",
                self.start, total_cases
            ));
        }
        let requested_end = match self.count {
            Some(count) => self
                .start
                .checked_add(count)
                .ok_or_else(|| "--case-start + --case-count overflows usize".to_owned())?,
            None => total_cases + 1,
        };
        Ok(requested_end.min(total_cases + 1))
    }
}

impl OracleSelection {
    fn from_cli(cli: &Cli) -> Result<Self, String> {
        if let Some(count) = cli.oracle_sample_count {
            if count == 0 {
                return Err("--oracle-sample-count must be greater than zero".to_owned());
            }
            if cli.oracle_case_start.is_some() || cli.oracle_case_count.is_some() {
                return Err(
                    "--oracle-sample-count cannot be combined with --case-start or --case-count"
                        .to_owned(),
                );
            }
            let seed = cli.seed.unwrap_or_else(generated_sample_seed);
            Ok(OracleSelection::Sample { count, seed })
        } else {
            OracleShard::from_cli(cli).map(OracleSelection::Exhaustive)
        }
    }

    fn selected_case_ids(self, total_cases: usize) -> Result<OracleCaseSelection, String> {
        match self {
            OracleSelection::Exhaustive(shard) => {
                let end = shard.end_exclusive(total_cases)?;
                Ok(OracleCaseSelection::Range {
                    start: shard.start,
                    end,
                    count: end - shard.start,
                })
            }
            OracleSelection::Sample { count, seed } => {
                let ids = sample_case_ids(total_cases, count, seed)?;
                Ok(OracleCaseSelection::Sample {
                    count: ids.len(),
                    ids,
                })
            }
        }
    }

    fn enforces_runtime_fixture_limit(self) -> bool {
        match self {
            OracleSelection::Sample { .. } => true,
            OracleSelection::Exhaustive(OracleShard { count, .. }) => count.is_some(),
        }
    }
}

enum OracleCaseSelection {
    Range {
        start: usize,
        end: usize,
        count: usize,
    },
    Sample {
        count: usize,
        ids: BTreeSet<usize>,
    },
}

impl OracleCaseSelection {
    fn len(&self) -> usize {
        match self {
            OracleCaseSelection::Range { count, .. }
            | OracleCaseSelection::Sample { count, .. } => *count,
        }
    }

    fn contains(&self, case_id: usize) -> bool {
        match self {
            OracleCaseSelection::Range { start, end, .. } => case_id >= *start && case_id < *end,
            OracleCaseSelection::Sample { ids, .. } => ids.contains(&case_id),
        }
    }

    fn is_complete(&self, visited: usize) -> bool {
        visited >= self.len()
    }
}

fn generated_sample_seed() -> u64 {
    rand::thread_rng().gen::<u64>()
}

fn sample_case_ids(
    total_cases: usize,
    requested: usize,
    seed: u64,
) -> Result<BTreeSet<usize>, String> {
    if total_cases == 0 {
        return Ok(BTreeSet::new());
    }

    let count = requested.min(total_cases);
    let mut rng = StdRng::seed_from_u64(seed);
    if count > total_cases / 2 {
        let mut ids = (1..=total_cases).collect::<Vec<_>>();
        ids.shuffle(&mut rng);
        ids.truncate(count);
        return Ok(ids.into_iter().collect());
    }

    let mut ids = BTreeSet::new();
    while ids.len() < count {
        ids.insert(rng.gen_range(1..=total_cases));
    }
    Ok(ids)
}

pub(crate) enum OracleEvent<'a> {
    Progress(&'a OracleProgress),
    Status(&'a OracleStatus),
    VerifyProgress(&'a OracleVerifyProgress),
}

struct Progress {
    total: usize,
    next_print: Instant,
}

impl Progress {
    fn new<F>(total: usize, on_progress: &mut F) -> Result<Self, String>
    where
        F: FnMut(&OracleProgress) -> Result<(), String>,
    {
        let progress = Progress {
            total,
            next_print: Instant::now() + Duration::from_secs(1),
        };
        progress.emit(0, on_progress)?;
        Ok(progress)
    }

    fn maybe_print<F>(&mut self, index: usize, on_progress: &mut F) -> Result<(), String>
    where
        F: FnMut(&OracleProgress) -> Result<(), String>,
    {
        if Instant::now() >= self.next_print {
            self.emit(index, on_progress)?;
            self.next_print = Instant::now() + Duration::from_secs(1);
        }
        Ok(())
    }

    fn finish<F>(&self, index: usize, on_progress: &mut F) -> Result<(), String>
    where
        F: FnMut(&OracleProgress) -> Result<(), String>,
    {
        self.emit(index, on_progress)
    }

    fn emit<F>(&self, index: usize, on_progress: &mut F) -> Result<(), String>
    where
        F: FnMut(&OracleProgress) -> Result<(), String>,
    {
        let percent = if self.total == 0 {
            100.0
        } else {
            (index as f64 * 100.0) / self.total as f64
        };
        let timestamp = current_timestamp();
        let line = format!("[{timestamp}] {percent:06.2}% {index}/{}", self.total);
        on_progress(&OracleProgress {
            timestamp,
            percent,
            index,
            total: self.total,
            line,
        })
    }
}

struct ReportWriter {
    writer: BufWriter<Box<dyn Write>>,
}

trait ReportSink {
    fn case(&mut self, case_id: usize, mask: usize, chain: &[Op]) -> Result<(), String>;
    fn step(&mut self, step: usize, worker: usize, op: Op) -> Result<(), String>;
    fn result(
        &mut self,
        step: usize,
        worker: usize,
        op: Op,
        result: &OpResult,
    ) -> Result<(), String>;
    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String>;
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
        let writer: Box<dyn Write> = if path.extension().and_then(|ext| ext.to_str()) == Some("gz")
        {
            Box::new(GzEncoder::new(file, Compression::default()))
        } else {
            Box::new(file)
        };
        let mut writer = BufWriter::new(writer);
        writer
            .write_all(REPORT_MAGIC)
            .map_err(|e| format!("failed to write report header: {e}"))?;
        write_u32(&mut writer, REPORT_VERSION)?;
        write_u64(&mut writer, chain_len as u64)?;
        write_string(&mut writer, run)?;
        Ok(ReportWriter { writer })
    }

    fn case(&mut self, case_id: usize, mask: usize, chain: &[Op]) -> Result<(), String> {
        write_u8(&mut self.writer, 1)?;
        write_var_u64(&mut self.writer, case_id as u64)?;
        write_var_u64(&mut self.writer, mask as u64)?;
        for op in chain {
            op.write_to(&mut self.writer)?;
        }
        Ok(())
    }

    fn step(&mut self, step: usize, worker: usize, op: Op) -> Result<(), String> {
        let _ = (step, worker, op);
        Ok(())
    }

    fn result(
        &mut self,
        step: usize,
        worker: usize,
        op: Op,
        result: &OpResult,
    ) -> Result<(), String> {
        let _ = (step, worker, op);
        write_u8(&mut self.writer, 2)?;
        write_op_result(&mut self.writer, result)
    }

    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String> {
        write_u8(&mut self.writer, 3)?;
        write_u8(&mut self.writer, snapshot.file.tag())?;
        write_u8(&mut self.writer, u8::from(snapshot.exists))?;
        if snapshot.exists {
            write_var_u64(&mut self.writer, snapshot.len)?;
            write_u64(&mut self.writer, snapshot.hash)?;
        }
        Ok(())
    }

    fn end(&mut self, cases: usize) -> Result<(), String> {
        write_u8(&mut self.writer, 4)?;
        write_var_u64(&mut self.writer, cases as u64)?;
        self.writer
            .flush()
            .map_err(|e| format!("failed to flush oracle report: {e}"))
    }
}

impl ReportSink for ReportWriter {
    fn case(&mut self, case_id: usize, mask: usize, chain: &[Op]) -> Result<(), String> {
        ReportWriter::case(self, case_id, mask, chain)
    }

    fn step(&mut self, step: usize, worker: usize, op: Op) -> Result<(), String> {
        ReportWriter::step(self, step, worker, op)
    }

    fn result(
        &mut self,
        step: usize,
        worker: usize,
        op: Op,
        result: &OpResult,
    ) -> Result<(), String> {
        ReportWriter::result(self, step, worker, op, result)
    }

    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String> {
        ReportWriter::snapshot(self, snapshot)
    }
}

#[derive(Default)]
struct MemoryCaseRecorder {
    records: Vec<ComparableRecord>,
}

impl ReportSink for MemoryCaseRecorder {
    fn case(&mut self, case_id: usize, mask: usize, _chain: &[Op]) -> Result<(), String> {
        self.records.push(ComparableRecord::Case {
            case_id: case_id as u64,
            mask: mask as u64,
        });
        Ok(())
    }

    fn step(&mut self, _step: usize, _worker: usize, _op: Op) -> Result<(), String> {
        Ok(())
    }

    fn result(
        &mut self,
        step: usize,
        worker: usize,
        op: Op,
        result: &OpResult,
    ) -> Result<(), String> {
        self.records.push(ComparableRecord::Result {
            step: step as u32,
            worker: worker as u32,
            op,
            rc: result.rc,
            err: result.err,
            data: result.data.clone(),
        });
        Ok(())
    }

    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String> {
        self.records.push(ComparableRecord::Snapshot {
            case_id: snapshot.case_id as u64,
            file: snapshot.file,
            rel: snapshot.rel.clone(),
            exists: snapshot.exists,
            len: snapshot.len,
            hash: snapshot.hash,
        });
        Ok(())
    }
}

struct TeeReportSink<'a> {
    report: &'a mut ReportWriter,
    memory: &'a mut MemoryCaseRecorder,
}

impl ReportSink for TeeReportSink<'_> {
    fn case(&mut self, case_id: usize, mask: usize, chain: &[Op]) -> Result<(), String> {
        self.report.case(case_id, mask, chain)?;
        self.memory.case(case_id, mask, chain)
    }

    fn step(&mut self, step: usize, worker: usize, op: Op) -> Result<(), String> {
        self.report.step(step, worker, op)?;
        self.memory.step(step, worker, op)
    }

    fn result(
        &mut self,
        step: usize,
        worker: usize,
        op: Op,
        result: &OpResult,
    ) -> Result<(), String> {
        self.report.result(step, worker, op, result)?;
        self.memory.result(step, worker, op, result)
    }

    fn snapshot(&mut self, snapshot: &FileSnapshot) -> Result<(), String> {
        self.report.snapshot(snapshot)?;
        self.memory.snapshot(snapshot)
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
        fixture_root: Arc<PathBuf>,
        fixture_id: usize,
        steps: &[ScheduledStep],
        report: &mut impl ReportSink,
    ) -> Result<(), String> {
        for (idx, step) in steps.iter().copied().enumerate() {
            let op = step.op;
            let worker = step.worker;
            report.step(idx + 1, worker, op)?;
            self.senders[worker]
                .send(WorkerMessage::Run(Job {
                    op,
                    case_root: Arc::clone(&case_root),
                    fixture_id,
                    fixture_root: Arc::clone(&fixture_root),
                }))
                .map_err(|e| e.to_string())?;
            let reply = self.reply_rx.recv().map_err(|e| e.to_string())?;
            report.result(idx + 1, worker, op, &reply.result)?;
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
    run_with_events(cli, |event| {
        match event {
            OracleEvent::Progress(progress) => println!("{}", progress.line),
            OracleEvent::VerifyProgress(progress) => println!("{}", progress.line),
            OracleEvent::Status(status) => println!("{}", status.message),
        }
        Ok(())
    })
}

pub(crate) fn run_with_events<F>(cli: Cli, mut on_event: F) -> Result<(), String>
where
    F: FnMut(OracleEvent<'_>) -> Result<(), String>,
{
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
    let selection = OracleSelection::from_cli(&cli)?;
    if let OracleSelection::Sample { count, seed } = selection {
        on_event(OracleEvent::Status(&OracleStatus {
            phase: "sampling",
            message: format!("sample seed={seed} count={count}"),
            expected: None,
            actual: Some(output.display().to_string()),
        }))?;
    }
    if let (OracleSelection::Sample { count, seed }, Some(expected)) =
        (selection, cli.oracle_expected.as_ref())
    {
        on_event(OracleEvent::Status(&OracleStatus {
            phase: "verifying",
            message: format!(
                "streaming sampled oracle from {} into {}",
                expected.display(),
                output.display()
            ),
            expected: Some(expected.display().to_string()),
            actual: Some(output.display().to_string()),
        }))?;
        run_sample_from_expected(
            &work_root,
            &theory_trial_1_fixture_root(&cli),
            len,
            &output,
            expected,
            count,
            seed,
            &mut |progress| on_event(OracleEvent::Progress(progress)),
        )?;
        on_event(OracleEvent::Status(&OracleStatus {
            phase: "verified",
            message: format!("sampled oracle verified against {}", expected.display()),
            expected: Some(expected.display().to_string()),
            actual: Some(output.display().to_string()),
        }))?;
        return Ok(());
    }
    let fixture_root = theory_trial_1_fixture_root(&cli);
    run_suite(
        &work_root,
        &fixture_root,
        len,
        &output,
        selection,
        &mut |progress| on_event(OracleEvent::Progress(progress)),
    )?;

    if let Some(path) = &cli.oracle_expected {
        on_event(OracleEvent::Status(&OracleStatus {
            phase: "verifying",
            message: format!("verifying {} against {}", output.display(), path.display()),
            expected: Some(path.display().to_string()),
            actual: Some(output.display().to_string()),
        }))?;
        compare_reports_with_progress(path, &output, &mut |progress| {
            on_event(OracleEvent::VerifyProgress(progress))
        })?;
        on_event(OracleEvent::Status(&OracleStatus {
            phase: "verified",
            message: format!("verified against {}", path.display()),
            expected: Some(path.display().to_string()),
            actual: Some(output.display().to_string()),
        }))?;
    } else if cli.oracle_output.is_none() {
        println!("oracle report written to {}", output.display());
    }

    Ok(())
}

pub(crate) fn verify_files(native_report: &Path, wasix_report: &Path) -> Result<(), String> {
    compare_reports(native_report, wasix_report)?;

    let native = read_report(native_report)?;
    let wasix = read_report(wasix_report)?;
    let wasix_snapshot_keys = wasix
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.file.snapshotted())
        .map(|snapshot| (snapshot.case_id, snapshot.file.tag()))
        .collect::<BTreeSet<_>>();
    let native_snapshots = native
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.file.snapshotted())
        .filter(|snapshot| wasix_snapshot_keys.contains(&(snapshot.case_id, snapshot.file.tag())))
        .collect::<Vec<_>>();
    let wasix_host_snapshots = wasix
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.file.snapshotted())
        .count();
    if native_snapshots.len() != wasix_host_snapshots {
        return Err(format!(
            "report snapshot count mismatch for requested shard: native={}, wasix={}",
            native_snapshots.len(),
            wasix_host_snapshots
        ));
    }

    let wasix_base = wasix_report.parent().ok_or_else(|| {
        format!(
            "cannot infer external file root from report path {}",
            wasix_report.display()
        )
    })?;

    let mut checked = 0usize;
    for expected in native_snapshots {
        let host_path = join_report_path(wasix_base, &wasix.run, &expected.rel)?;
        verify_snapshot_file(expected, &host_path)?;
        checked += 1;
    }

    println!("oracle external file verification ok: checked {checked} file snapshots");
    Ok(())
}

pub(crate) fn catalog_key() -> String {
    format!("{:016x}", stable_hash(catalog_signature().as_bytes()))
}

pub(crate) fn catalog_syscalls() -> String {
    op_catalog()
        .iter()
        .map(|spec| spec.name())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn catalog_report() -> String {
    let mut out = String::new();
    out.push_str(&format!("oracle_catalog_key={}\n", catalog_key()));
    out.push_str(&format!("report_version={REPORT_VERSION}\n"));
    out.push_str(&format!("workers={WORKERS}\n"));
    out.push_str(&format!("fd_slots={FD_SLOTS}\n"));
    out.push_str(&format!("require_mutation={REQUIRE_MUTATION}\n"));
    out.push_str(&format!("normalization_version={NORMALIZATION_VERSION}\n"));
    out.push_str(&format!(
        "error_normalization_version={ERROR_NORMALIZATION_VERSION}\n"
    ));
    out.push_str(&format!(
        "files={}\n",
        FILES
            .iter()
            .map(|file| file.name())
            .collect::<Vec<_>>()
            .join(",")
    ));
    out.push_str(&format!(
        "snapshots={}\n",
        SNAPSHOT_FILES
            .iter()
            .map(|file| file.name())
            .collect::<Vec<_>>()
            .join(",")
    ));
    out.push_str("op_catalog:\n");
    for spec in op_catalog() {
        out.push_str(&format!("- {}\n", spec.name()));
    }
    out.push_str(&format!("syscalls={}\n", catalog_syscalls()));
    out.push_str(&format!("concrete_ops={}\n", concrete_ops().len()));
    out
}

fn catalog_signature() -> String {
    let mut sig = String::new();
    sig.push_str(&format!("report_version={REPORT_VERSION}\n"));
    sig.push_str(&format!("workers={WORKERS}\n"));
    sig.push_str(&format!("fd_slots={FD_SLOTS}\n"));
    sig.push_str(&format!("require_mutation={REQUIRE_MUTATION}\n"));
    sig.push_str(&format!("normalization_version={NORMALIZATION_VERSION}\n"));
    sig.push_str(&format!(
        "error_normalization_version={ERROR_NORMALIZATION_VERSION}\n"
    ));
    sig.push_str(&format!("stderr_marker_len={}\n", STDERR_MARKER.len()));
    sig.push_str(&format!(
        "files={}\n",
        FILES
            .iter()
            .map(|file| format!("{}:{}", file.name(), file.tag()))
            .collect::<Vec<_>>()
            .join(",")
    ));
    sig.push_str(&format!(
        "snapshots={}\n",
        SNAPSHOT_FILES
            .iter()
            .map(|file| format!("{}:{}", file.name(), file.tag()))
            .collect::<Vec<_>>()
            .join(",")
    ));
    sig.push_str("op_catalog:\n");
    for spec in op_catalog() {
        sig.push_str(&spec.name());
        sig.push('\n');
    }
    sig.push_str("concrete_ops:\n");
    for op in concrete_ops() {
        sig.push_str(&op.pseudocode());
        sig.push('\n');
    }
    sig
}

fn chain_is_enabled(chain: &[Op]) -> bool {
    !REQUIRE_MUTATION || chain.iter().any(|op| op.is_mutation())
}

fn scheduled_steps(chain: &[Op], scheduler: &Scheduler, mask: usize) -> Vec<ScheduledStep> {
    chain
        .iter()
        .copied()
        .zip(scheduler.workers(chain.len(), mask))
        .map(|(op, worker)| ScheduledStep { worker, op })
        .collect()
}

fn canonical_slot(worker_slots: &mut [Option<usize>; FD_SLOTS], slot: usize) -> usize {
    match worker_slots[slot] {
        Some(canonical_slot) => canonical_slot,
        None => {
            let canonical_slot = worker_slots
                .iter()
                .filter(|mapped| mapped.is_some())
                .count();
            worker_slots[slot] = Some(canonical_slot);
            canonical_slot
        }
    }
}

fn normalize_scheduled_case(steps: &[ScheduledStep]) -> Vec<ScheduledStep> {
    let mut opened = [[false; FD_SLOTS]; WORKERS];
    let mut slot_maps = [[None; FD_SLOTS]; WORKERS];
    let mut file_map = [None; 2];
    let mut normalized = Vec::with_capacity(steps.len());

    for step in steps {
        let slot = step.op.slot();
        if step.op.requires_fd() {
            if let Some(slot) = slot {
                if !opened[step.worker][slot] {
                    continue;
                }
            }
        }

        let op = match step.op {
            Op::Dup { src_slot, dst_slot } => {
                let worker_slots = &mut slot_maps[step.worker];
                let canonical_src_slot = canonical_slot(worker_slots, src_slot);
                let canonical_dst_slot = canonical_slot(worker_slots, dst_slot);
                Op::Dup {
                    src_slot: canonical_src_slot,
                    dst_slot: canonical_dst_slot,
                }
            }
            _ => {
                if let Some(slot) = slot {
                    let canonical_slot = canonical_slot(&mut slot_maps[step.worker], slot);
                    step.op.with_slot(canonical_slot)
                } else {
                    step.op
                }
            }
        };

        let op = if let Some(file) = step.op.file() {
            if let Some(file_index) = file.canonicalizable_index() {
                let canonical_file = match file_map[file_index] {
                    Some(canonical_file) => canonical_file,
                    None => {
                        let canonical_file =
                            FileId::from_canonicalizable_index(file_map.iter().flatten().count());
                        file_map[file_index] = Some(canonical_file);
                        canonical_file
                    }
                };
                op.with_file(canonical_file)
            } else {
                op
            }
        } else {
            op
        };

        normalized.push(ScheduledStep {
            worker: step.worker,
            op,
        });

        match step.op {
            Op::Open { slot, .. } => {
                opened[step.worker][slot] = true;
            }
            Op::Close { .. } => {}
            Op::Write { .. } => {}
            Op::Read { .. } => {}
            Op::Seek { .. } => {}
            Op::Fstat { .. } => {}
            Op::Stat { .. } => {}
            Op::ReadDir => {}
            Op::WriteStderr => {}
            Op::Dup { dst_slot, .. } => {
                opened[step.worker][dst_slot] = true;
            }
            Op::Delete { .. } => {}
        }
    }

    normalized
}

fn scheduled_case_is_enabled(steps: &[ScheduledStep]) -> bool {
    normalize_scheduled_case(steps) == steps
}

fn known_scheduled_case_count(len: usize) -> Option<usize> {
    match len {
        1 => Some(4),
        2 => Some(72),
        3 => Some(1_636),
        4 => Some(43_248),
        5 => Some(1_261_228),
        _ => None,
    }
}

fn active_catalog_uses_fixtures() -> bool {
    FILES.iter().copied().any(FileId::is_fixture)
}

fn ensure_selected_fixtures_available(selection: &OracleCaseSelection) -> Result<(), String> {
    if !active_catalog_uses_fixtures() {
        return Ok(());
    }
    let selected = selection.len();
    if selected > THEORY_TRIAL_1_FIXTURE_COUNT {
        return Err(format!(
            "selected oracle case count {selected} exceeds fixture pool size {}; shard/sample the run to at most {} cases or generate more fixtures",
            THEORY_TRIAL_1_FIXTURE_COUNT,
            THEORY_TRIAL_1_FIXTURE_COUNT
        ));
    }
    Ok(())
}

fn selected_run_fixture_id(total_cases: usize, case_id: usize, selected_ordinal: usize) -> usize {
    // Small levels can keep the old absolute mapping so subsets compare against
    // full bundled reports. Larger levels allocate fixtures per selected run.
    if total_cases <= THEORY_TRIAL_1_FIXTURE_COUNT {
        case_id
    } else {
        selected_ordinal
    }
}

pub(crate) fn prepare_theory_trial_fixtures(root: &Path) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|e| format!("failed to create {}: {e}", root.display()))?;
    for index in 1..=THEORY_TRIAL_1_FIXTURE_COUNT {
        let name = theory_trial_1_fixture_name(index);
        let path = root.join(&name);
        let expected = theory_trial_1_fixture_bytes(&name);
        match fs::read(&path) {
            Ok(existing) if existing == expected => {}
            Ok(_) | Err(_) => {
                fs::write(&path, &expected)
                    .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
            }
        }
    }
    Ok(())
}

fn theory_trial_1_fixture_root(cli: &Cli) -> PathBuf {
    if let Some(root) = &cli.oracle_fixtures {
        return root.clone();
    }
    let package_root = PathBuf::from("/oracle/theory-trial-1-fixtures");
    if package_root.exists() {
        return package_root;
    }
    let local_asset_root = PathBuf::from("package-assets/theory-trial-1-fixtures");
    if local_asset_root.exists() {
        return local_asset_root;
    }
    PathBuf::from("oracle-cache/theory-trial-1-fixtures")
}

fn theory_trial_1_fixture_name(index: usize) -> String {
    format!("file{index}")
}

fn fixture_path(root: &Path, fixture_id: usize, fixture: FileId) -> PathBuf {
    root.join(theory_trial_1_fixture_name(fixture_index(fixture_id, fixture)))
}

fn fixture_index(fixture_id: usize, fixture: FileId) -> usize {
    match fixture {
        FileId::Fixture1 => fixture_id,
        FileId::Fixture2 => {
            let hash = stable_hash(&fixture_id.to_le_bytes()) ^ 0x4653_5832_u64;
            (hash as usize % THEORY_TRIAL_1_FIXTURE_COUNT) + 1
        }
        FileId::A | FileId::B | FileId::C => panic!("not a fixture file: {fixture:?}"),
    }
}

fn theory_trial_1_fixture_bytes(name: &str) -> Vec<u8> {
    let hash = stable_hash(name.as_bytes());
    let len = (hash as usize % 1024) + 1;
    let header = format!("FSX_THEORY_TRIAL_1_FIXTURE name={name} hash={hash:016x}\n");
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&hash.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn run_suite<F>(
    root: &Path,
    fixture_root: &Path,
    len: usize,
    output: &Path,
    selection: OracleSelection,
    on_progress: &mut F,
) -> Result<(), String>
where
    F: FnMut(&OracleProgress) -> Result<(), String>,
{
    let generator = ChainGenerator::new();
    let scheduler = Scheduler::new(WORKERS);
    let mut total_cases = 0usize;
    let mut counted_chains = 0usize;
    let mut emit_count_progress = |progress: &OracleProgress| {
        on_progress(&OracleProgress {
            timestamp: progress.timestamp.clone(),
            percent: progress.percent,
            index: progress.index,
            total: progress.total,
            line: format!("counting chains {}", progress.line),
        })
    };
    let mut count_progress = Progress::new(generator.chain_count(len)?, &mut emit_count_progress)?;
    generator.for_each(len, |chain| {
        counted_chains += 1;
        if chain_is_enabled(chain) {
            let enabled_masks = (0usize..scheduler.schedule_count(chain.len()))
                .filter(|mask| {
                    let steps = scheduled_steps(chain, &scheduler, *mask);
                    scheduled_case_is_enabled(&steps)
                })
                .count();
            total_cases += enabled_masks;
        }
        count_progress.maybe_print(counted_chains, &mut emit_count_progress)?;
        Ok(true)
    })?;
    count_progress.finish(counted_chains, &mut emit_count_progress)?;
    let selected_cases = selection.selected_case_ids(total_cases)?;
    if selection.enforces_runtime_fixture_limit() {
        ensure_selected_fixtures_available(&selected_cases)?;
    }

    let run = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid oracle run root {}", root.display()))?;
    let mut report = ReportWriter::create(output, len, run)?;

    let runner = WorkerPool::start(WORKERS);
    let fixture_root = Arc::new(fixture_root.to_path_buf());
    let mut progress = Progress::new(selected_cases.len(), on_progress)?;

    let mut case_id = 0usize;
    let mut selected_index = 0usize;
    generator.for_each(len, |chain| {
        if selected_cases.is_complete(selected_index) {
            return Ok(false);
        }
        if !chain_is_enabled(chain) {
            return Ok(true);
        }
        for mask in 0usize..scheduler.schedule_count(chain.len()) {
            let steps = scheduled_steps(&chain, &scheduler, mask);
            if !scheduled_case_is_enabled(&steps) {
                continue;
            }
            case_id += 1;
            if !selected_cases.contains(case_id) {
                continue;
            }
            let fixture_id = selected_run_fixture_id(total_cases, case_id, selected_index + 1);
            let case_name = format!("case-{case_id:06}");
            let case_root = root.join(&case_name);
            fs::create_dir_all(&case_root)
                .map_err(|e| format!("failed to create {}: {e}", case_root.display()))?;
            cleanup_tmp_files(&case_root)?;
            report.case(case_id, mask, chain)?;
            runner.reset_state();
            runner.run_case(
                Arc::new(case_root.clone()),
                Arc::clone(&fixture_root),
                fixture_id,
                &steps,
                &mut report,
            )?;
            snapshot(
                &case_root,
                &fixture_root,
                case_id,
                fixture_id,
                &case_name,
                chain,
                &mut report,
            )?;
            runner.reset_state();
            cleanup_tmp_files(&case_root)?;
            selected_index += 1;
            progress.maybe_print(selected_index, on_progress)?;
            if selected_cases.is_complete(selected_index) {
                return Ok(false);
            }
        }
        Ok(true)
    })?;

    runner.stop();
    progress.finish(selected_index, on_progress)?;

    report.end(total_cases)
}

fn run_sample_from_expected<F>(
    root: &Path,
    fixture_root: &Path,
    len: usize,
    output: &Path,
    expected: &Path,
    target_count: usize,
    seed: u64,
    on_progress: &mut F,
) -> Result<(), String>
where
    F: FnMut(&OracleProgress) -> Result<(), String>,
{
    if target_count == 0 {
        return Err("--oracle-sample-count must be greater than zero".to_owned());
    }
    let total_cases = known_scheduled_case_count(len).ok_or_else(|| {
        format!("streamed sampling does not have a cached case count for L={len}")
    })?;
    let selected_case_ids = sample_case_ids(total_cases, target_count, seed)?;
    let selected_cases = OracleCaseSelection::Sample {
        count: selected_case_ids.len(),
        ids: selected_case_ids.clone(),
    };
    ensure_selected_fixtures_available(&selected_cases)?;
    let selected_target = selected_case_ids.len();
    let mut reader = ReportReader::open(expected)?;
    if reader.chain_len != len {
        return Err(format!(
            "oracle chain length mismatch: expected report has {}, requested {}",
            reader.chain_len, len
        ));
    }

    let run = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid oracle run root {}", root.display()))?;
    let mut report = ReportWriter::create(output, len, run)?;
    let runner = WorkerPool::start(WORKERS);
    let fixture_root = Arc::new(fixture_root.to_path_buf());
    let mut progress = Progress::new(total_cases, &mut |progress| {
        on_progress(&sample_scan_progress(progress, 0, selected_target))
    })?;

    let mut expected_records = Vec::new();
    let mut actual_records: Option<Vec<ComparableRecord>> = None;
    let mut selected_case_id = None;
    let mut selected_count = 0usize;
    let mut current_case_id = 0usize;

    while let Some((record, _)) = reader.read_record()? {
        match record {
            ComparableRecord::Case { case_id, mask } => {
                finish_streamed_sample_case(
                    selected_case_id.take(),
                    &mut expected_records,
                    &mut actual_records,
                )?;
                current_case_id = case_id as usize;
                progress.maybe_print(current_case_id, &mut |progress| {
                    on_progress(&sample_scan_progress(
                        progress,
                        selected_count,
                        selected_target,
                    ))
                })?;

                if selected_case_ids.contains(&current_case_id) {
                    selected_count += 1;
                    let fixture_id =
                        selected_run_fixture_id(total_cases, case_id as usize, selected_count);
                    selected_case_id = Some(case_id);
                    expected_records.push(ComparableRecord::Case { case_id, mask });

                    let case_name = format!("case-{case_id:06}");
                    let case_root = root.join(&case_name);
                    fs::create_dir_all(&case_root)
                        .map_err(|e| format!("failed to create {}: {e}", case_root.display()))?;
                    cleanup_tmp_files(&case_root)?;
                    runner.reset_state();

                    let mut memory = MemoryCaseRecorder::default();
                    {
                        let mut sink = TeeReportSink {
                            report: &mut report,
                            memory: &mut memory,
                        };
                        sink.case(case_id as usize, mask as usize, &reader.current_chain)?;
                        runner.run_case(
                            Arc::new(case_root.clone()),
                            Arc::clone(&fixture_root),
                            fixture_id,
                            &reader.current_steps,
                            &mut sink,
                        )?;
                        snapshot(
                            &case_root,
                            &fixture_root,
                            case_id as usize,
                            fixture_id,
                            &case_name,
                            &reader.current_chain,
                            &mut sink,
                        )?;
                    }
                    runner.reset_state();
                    cleanup_tmp_files(&case_root)?;
                    actual_records = Some(memory.records);
                }
            }
            ComparableRecord::Result { .. } | ComparableRecord::Snapshot { .. } => {
                if selected_case_id.is_some() {
                    expected_records.push(record);
                }
            }
            ComparableRecord::End { cases } => {
                finish_streamed_sample_case(
                    selected_case_id.take(),
                    &mut expected_records,
                    &mut actual_records,
                )?;
                let cases = cases as usize;
                if cases != total_cases {
                    return Err(format!(
                        "expected report total cases {cases} does not match active catalog count {total_cases}"
                    ));
                }
                progress.finish(cases, &mut |progress| {
                    on_progress(&sample_scan_progress(
                        progress,
                        selected_count,
                        selected_target,
                    ))
                })?;
                if selected_count != selected_target {
                    return Err(format!(
                        "sampled {selected_count} cases but expected {selected_target}"
                    ));
                }
                report.end(cases)?;
                runner.stop();
                return Ok(());
            }
        }
    }

    runner.stop();
    Err(format!(
        "expected oracle report {} ended without an end record after case {}",
        expected.display(),
        current_case_id
    ))
}

fn sample_scan_progress(
    progress: &OracleProgress,
    selected_count: usize,
    target_count: usize,
) -> OracleProgress {
    OracleProgress {
        timestamp: progress.timestamp.clone(),
        percent: progress.percent,
        index: progress.index,
        total: progress.total,
        line: format!(
            "sample scan {} selected={selected_count} target={target_count}",
            progress.line
        ),
    }
}

fn finish_streamed_sample_case(
    case_id: Option<u64>,
    expected_records: &mut Vec<ComparableRecord>,
    actual_records: &mut Option<Vec<ComparableRecord>>,
) -> Result<(), String> {
    let Some(case_id) = case_id else {
        return Ok(());
    };
    let actual = actual_records
        .take()
        .ok_or_else(|| format!("missing actual records for sampled case {case_id}"))?;
    compare_streamed_sample_case(case_id, expected_records, &actual)?;
    expected_records.clear();
    Ok(())
}

fn compare_streamed_sample_case(
    case_id: u64,
    expected: &[ComparableRecord],
    actual: &[ComparableRecord],
) -> Result<(), String> {
    if expected.len() != actual.len() {
        return Err(format!(
            "oracle found mismatch in sampled case {case_id}: record count mismatch: expected {}, actual {}",
            expected.len(),
            actual.len()
        ));
    }
    for (idx, (expected_record, actual_record)) in expected.iter().zip(actual.iter()).enumerate() {
        if expected_record != actual_record {
            if ignore_known_error(expected, idx, expected_record, actual_record) {
                continue;
            }
            return Err(format!(
                "oracle found mismatch in sampled case {case_id}\n\n{}",
                format_mismatch(idx + 1, idx, expected, expected_record, actual_record)
            ));
        }
    }
    Ok(())
}

fn execute_job(worker: usize, state: &Arc<Mutex<State>>, job: Job) -> Reply {
    let result = match job.op {
        Op::Open { slot, file, mode } => {
            let path = file.resolved_path(&job.case_root, &job.fixture_root, job.fixture_id);
            let result = open_file(&path, mode);
            match result {
                Ok(file) => {
                    let mut state = state.lock().unwrap();
                    let worker_state = &mut state.workers[worker];
                    let old = std::mem::replace(&mut worker_state.slots[slot], FdSlot::Open(file));
                    if let FdSlot::Open(old) = old {
                        // Mirrors `fd_slot = open(...)`: the old numeric fd leaks if overwritten.
                        worker_state.leaked.push(old);
                    }
                    OpResult::ok(-1)
                }
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Close { slot } => {
            let mut state = state.lock().unwrap();
            let worker_state = &mut state.workers[worker];
            match std::mem::replace(&mut worker_state.slots[slot], FdSlot::NeverOpened) {
                FdSlot::Open(file) => {
                    let raw_fd = file.as_raw_fd();
                    drop(file);
                    worker_state.slots[slot] = FdSlot::Closed(raw_fd);
                    OpResult::ok(-1)
                }
                FdSlot::Closed(raw_fd) => {
                    worker_state.slots[slot] = FdSlot::Closed(raw_fd);
                    OpResult::err(OracleErr::BadFd)
                }
                FdSlot::NeverOpened => {
                    worker_state.slots[slot] = FdSlot::NeverOpened;
                    OpResult::err(OracleErr::BadFd)
                }
            }
        }
        Op::Write { slot, payload } => {
            let mut state = state.lock().unwrap();
            let buf = write_payload(slot, payload);
            match &mut state.workers[worker].slots[slot] {
                FdSlot::Open(file) => match file.write(&buf) {
                    Ok(n) => OpResult::ok(n as i64),
                    Err(e) => OpResult::err(errcode(&e)),
                },
                FdSlot::Closed(raw_fd) => raw_write(*raw_fd, &buf),
                FdSlot::NeverOpened => OpResult::err(OracleErr::BadFd),
            }
        }
        Op::Read { slot, size } => {
            let mut state = state.lock().unwrap();
            let mut buf = vec![0u8; size.len()];
            match &mut state.workers[worker].slots[slot] {
                FdSlot::Open(file) => match file.read(&mut buf) {
                    Ok(n) => OpResult::ok_data(n as i64, buf[..n].to_vec()),
                    Err(e) => OpResult::err(errcode(&e)),
                },
                FdSlot::Closed(raw_fd) => raw_read(*raw_fd, &mut buf),
                FdSlot::NeverOpened => OpResult::err(OracleErr::BadFd),
            }
        }
        Op::WriteStderr => match std::io::stderr().write_all(STDERR_MARKER) {
            Ok(()) => OpResult::ok(STDERR_MARKER.len() as i64),
            Err(e) => OpResult::err(errcode(&e)),
        },
        Op::Dup { src_slot, dst_slot } => {
            let mut state = state.lock().unwrap();
            let worker_state = &mut state.workers[worker];
            let raw_fd = match &worker_state.slots[src_slot] {
                FdSlot::Open(file) => file.as_raw_fd(),
                FdSlot::Closed(raw_fd) => *raw_fd,
                FdSlot::NeverOpened => {
                    return Reply {
                        result: OpResult::err(OracleErr::BadFd),
                    };
                }
            };

            match raw_dup(raw_fd) {
                Ok(dup_fd) => {
                    let dup_file = unsafe { File::from_raw_fd(dup_fd) };
                    let old = std::mem::replace(
                        &mut worker_state.slots[dst_slot],
                        FdSlot::Open(dup_file),
                    );
                    if let FdSlot::Open(old) = old {
                        worker_state.leaked.push(old);
                    }
                    OpResult::ok(0)
                }
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Delete { file } => {
            let path = file.resolved_path(&job.case_root, &job.fixture_root, job.fixture_id);
            match fs::remove_file(&path) {
                Ok(()) => OpResult::ok(-1),
                Err(e) => OpResult::err(errcode(&e)),
            }
        }
        Op::Seek { slot, target } => {
            let mut state = state.lock().unwrap();
            let seek_from = match target {
                SeekTarget::Start => SeekFrom::Start(0),
                SeekTarget::End => SeekFrom::End(0),
            };
            match &mut state.workers[worker].slots[slot] {
                FdSlot::Open(file) => match file.seek(seek_from) {
                    Ok(pos) => OpResult::ok(pos as i64),
                    Err(e) => OpResult::err(errcode(&e)),
                },
                FdSlot::Closed(raw_fd) => raw_seek(*raw_fd, target),
                FdSlot::NeverOpened => OpResult::err(OracleErr::BadFd),
            }
        }
        Op::Fstat { slot } => {
            let state = state.lock().unwrap();
            match &state.workers[worker].slots[slot] {
                FdSlot::Open(file) => match file.metadata() {
                    Ok(metadata) => OpResult::ok(metadata.len() as i64),
                    Err(e) => OpResult::err(errcode(&e)),
                },
                FdSlot::Closed(raw_fd) => raw_fstat(*raw_fd),
                FdSlot::NeverOpened => OpResult::err(OracleErr::BadFd),
            }
        }
        Op::Stat { file } => {
            let path = file.resolved_path(&job.case_root, &job.fixture_root, job.fixture_id);
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
        OpenMode::ReadOnlyExisting => OpenOptions::new().read(true).open(path),
    }
}

fn raw_write(fd: RawFd, buf: &[u8]) -> OpResult {
    with_borrowed_raw_file(fd, |file| match file.write(buf) {
        Ok(n) => OpResult::ok(n as i64),
        Err(e) => OpResult::err(errcode(&e)),
    })
}

fn raw_read(fd: RawFd, buf: &mut [u8]) -> OpResult {
    with_borrowed_raw_file(fd, |file| match file.read(buf) {
        Ok(n) => OpResult::ok_data(n as i64, buf[..n].to_vec()),
        Err(e) => OpResult::err(errcode(&e)),
    })
}

fn raw_seek(fd: RawFd, target: SeekTarget) -> OpResult {
    let seek_from = match target {
        SeekTarget::Start => SeekFrom::Start(0),
        SeekTarget::End => SeekFrom::End(0),
    };
    with_borrowed_raw_file(fd, |file| match file.seek(seek_from) {
        Ok(pos) => OpResult::ok(pos as i64),
        Err(e) => OpResult::err(errcode(&e)),
    })
}

fn raw_fstat(fd: RawFd) -> OpResult {
    with_borrowed_raw_file(fd, |file| match file.metadata() {
        Ok(metadata) => OpResult::ok(metadata.len() as i64),
        Err(e) => OpResult::err(errcode(&e)),
    })
}

fn with_borrowed_raw_file<F>(fd: RawFd, f: F) -> OpResult
where
    F: FnOnce(&mut File) -> OpResult,
{
    let mut file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
    f(&mut file)
}

unsafe extern "C" {
    fn dup(fd: i32) -> i32;
}

fn raw_dup(fd: RawFd) -> std::io::Result<RawFd> {
    let result = unsafe { dup(fd) };
    if result < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn concrete_ops() -> Vec<Op> {
    let mut ops = Vec::new();
    for spec in op_catalog() {
        if *spec == OpSpec::Dup {
            for src_slot in 0..FD_SLOTS {
                for dst_slot in 0..FD_SLOTS {
                    ops.push(Op::Dup { src_slot, dst_slot });
                }
            }
        } else if spec.needs_file() && !spec.needs_fd() {
            for file in FILES.iter().copied() {
                ops.push(expand_op(*spec, 0, Some(file)));
            }
        } else if !spec.needs_fd() {
            ops.push(expand_op(*spec, 0, None));
        } else {
            for slot in 0..FD_SLOTS {
                if spec.needs_file() {
                    for file in files_for_spec(*spec) {
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

fn files_for_spec(spec: OpSpec) -> Vec<FileId> {
    match spec {
        OpSpec::Open(OpenMode::ReadOnlyExisting) => {
            FILES.iter().chain(FIXTURE_FILES.iter()).copied().collect()
        }
        OpSpec::Open(OpenMode::ReadWriteCreate)
        | OpSpec::Open(OpenMode::AppendCreate)
        | OpSpec::Open(OpenMode::ReadWriteTruncate)
        | OpSpec::Open(OpenMode::ReadWriteCreateNew)
        | OpSpec::Delete
        | OpSpec::Stat => FILES.to_vec(),
        OpSpec::Close
        | OpSpec::Write(_)
        | OpSpec::Read(_)
        | OpSpec::Seek(_)
        | OpSpec::Fstat
        | OpSpec::ReadDir
        | OpSpec::WriteStderr
        | OpSpec::Dup => Vec::new(),
    }
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
        OpSpec::Dup => panic!("dup ops are expanded across source and destination slots"),
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
    for file in FILES
        .iter()
        .copied()
        .filter(|file| !file.snapshotted() && !file.is_fixture())
    {
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
        let file_name = entry.file_name();
        if is_ignored_dir_entry(&file_name) {
            continue;
        }
        entries.push(file_name.to_string_lossy().into_owned());
    }
    entries.sort();
    Ok(entries.join("\n").into_bytes())
}

fn is_ignored_dir_entry(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .map(|name| name.starts_with(".nfs"))
        .unwrap_or(false)
}

fn empty_slots() -> Vec<FdSlot> {
    (0..FD_SLOTS).map(|_| FdSlot::NeverOpened).collect()
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
    fixture_root: &Path,
    case_id: usize,
    fixture_id: usize,
    case_name: &str,
    chain: &[Op],
    report: &mut impl ReportSink,
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
    for fixture in fixture_snapshots_for_chain(chain) {
        let path = fixture_path(fixture_root, fixture_id, fixture);
        match fs::read(&path) {
            Ok(data) => {
                report.snapshot(&FileSnapshot {
                    case_id,
                    file: fixture,
                    rel: PathBuf::from(format!("{case_name}/{}", fixture.name())),
                    exists: true,
                    len: data.len() as u64,
                    hash: stable_hash(&data),
                })?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                report.snapshot(&FileSnapshot {
                    case_id,
                    file: fixture,
                    rel: PathBuf::from(format!("{case_name}/{}", fixture.name())),
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

fn fixture_snapshots_for_chain(chain: &[Op]) -> BTreeSet<FileId> {
    let mut fixtures = BTreeSet::new();
    for op in chain {
        match op {
            Op::Open { file, .. } => {
                if matches!(file, FileId::Fixture1 | FileId::Fixture2) {
                    fixtures.insert(*file);
                }
            }
            Op::Close { .. } => {}
            Op::Write { .. } => {}
            Op::Read { .. } => {}
            Op::Seek { .. } => {}
            Op::Fstat { .. } => {}
            Op::Stat { .. } => {}
            Op::ReadDir => {}
            Op::WriteStderr => {}
            Op::Dup { .. } => {}
            Op::Delete { .. } => {}
        }
    }
    fixtures
}

fn compare_reports(expected_path: &Path, actual_path: &Path) -> Result<(), String> {
    compare_reports_with_progress(expected_path, actual_path, &mut |_| Ok(()))
}

fn compare_reports_with_progress<F>(
    expected_path: &Path,
    actual_path: &Path,
    on_verify_progress: &mut F,
) -> Result<(), String>
where
    F: FnMut(&OracleVerifyProgress) -> Result<(), String>,
{
    let actual = read_report(actual_path)?;
    let actual_case_ids = report_case_ids(&actual.records);
    let actual_total_cases = report_total_cases(&actual.records).unwrap_or(actual_case_ids.len());
    let (expected_chain_len, expected_records) = read_report_records_for_case_ids(
        expected_path,
        &actual_case_ids,
        actual_total_cases,
        on_verify_progress,
    )?;
    if expected_chain_len != actual.chain_len {
        return Err(format!(
            "oracle chain length mismatch: expected {}, actual {}",
            expected_chain_len, actual.chain_len
        ));
    }
    if expected_records.len() != actual.records.len() {
        return Err(format!(
            "oracle record count mismatch: expected {}, actual {}",
            expected_records.len(),
            actual.records.len()
        ));
    }
    let mut mismatches = Vec::new();
    for (idx, (expected_record, actual_record)) in expected_records
        .iter()
        .zip(actual.records.iter())
        .enumerate()
    {
        if expected_record != actual_record {
            if ignore_known_error(&expected_records, idx, expected_record, actual_record) {
                continue;
            }
            mismatches.push(format_mismatch(
                idx + 1,
                idx,
                &expected_records,
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

fn report_case_ids(records: &[ComparableRecord]) -> BTreeSet<u64> {
    records
        .iter()
        .filter_map(|record| {
            if let ComparableRecord::Case { case_id, .. } = record {
                Some(*case_id)
            } else {
                None
            }
        })
        .collect()
}

fn report_total_cases(records: &[ComparableRecord]) -> Option<usize> {
    records.iter().rev().find_map(|record| {
        if let ComparableRecord::End { cases } = record {
            Some(*cases as usize)
        } else {
            None
        }
    })
}

fn read_report_records_for_case_ids(
    path: &Path,
    case_ids: &BTreeSet<u64>,
    total_cases: usize,
    on_verify_progress: &mut impl FnMut(&OracleVerifyProgress) -> Result<(), String>,
) -> Result<(usize, Vec<ComparableRecord>), String> {
    let mut reader = ReportReader::open(path)?;
    let chain_len = reader.chain_len;
    let mut selected = Vec::new();
    let mut include_current_case = false;
    let mut current_case = 0usize;
    let mut progress = Progress::new(total_cases, &mut |progress| {
        on_verify_progress(&OracleVerifyProgress {
            timestamp: progress.timestamp.clone(),
            percent: progress.percent,
            index: progress.index,
            total: progress.total,
            line: format!("verify {} selected=0/{}", progress.line, case_ids.len()),
        })
    })?;
    let mut found_cases = 0usize;

    while let Some(record) = reader.read_record()?.map(|(record, _)| record) {
        match &record {
            ComparableRecord::Case { case_id, .. } => {
                current_case = *case_id as usize;
                include_current_case = case_ids.contains(case_id);
                progress.maybe_print(current_case, &mut |progress| {
                    on_verify_progress(&OracleVerifyProgress {
                        timestamp: progress.timestamp.clone(),
                        percent: progress.percent,
                        index: progress.index,
                        total: progress.total,
                        line: format!(
                            "verify {} selected={found_cases}/{}",
                            progress.line,
                            case_ids.len()
                        ),
                    })
                })?;
                if include_current_case {
                    found_cases += 1;
                    selected.push(record);
                }
            }
            ComparableRecord::Result { .. } => {
                if include_current_case {
                    selected.push(record);
                }
            }
            ComparableRecord::Snapshot { case_id, .. } => {
                if case_ids.contains(case_id) {
                    selected.push(record);
                }
            }
            ComparableRecord::End { .. } => selected.push(record),
        }
    }
    progress.finish(current_case.max(total_cases), &mut |progress| {
        on_verify_progress(&OracleVerifyProgress {
            timestamp: progress.timestamp.clone(),
            percent: progress.percent,
            index: progress.index,
            total: progress.total,
            line: format!(
                "verify {} selected={found_cases}/{}",
                progress.line,
                case_ids.len()
            ),
        })
    })?;

    Ok((chain_len, selected))
}

#[derive(Clone, Copy)]
struct KnownOpenFd {
    file: FileId,
    deleted_after_open: bool,
}

fn ignore_known_error(
    expected_records: &[ComparableRecord],
    record_idx: usize,
    expected: &ComparableRecord,
    actual: &ComparableRecord,
) -> bool {
    // TODO: Remove this temporary filter after
    // https://github.com/wasmerio/wasmer/pull/6467 is merged and deployed.
    let (
        ComparableRecord::Result {
            step: expected_step,
            worker: expected_worker,
            op: expected_op,
            rc: _,
            err: OracleErr::None,
            ..
        },
        ComparableRecord::Result {
            step: actual_step,
            worker: actual_worker,
            op: actual_op,
            rc: -1,
            err: OracleErr::NotFound,
            ..
        },
    ) = (expected, actual)
    else {
        return false;
    };
    if expected_step != actual_step || expected_worker != actual_worker || expected_op != actual_op
    {
        return false;
    }

    let slot = match expected_op {
        Op::Open { .. } => return false,
        Op::Close { .. } => return false,
        Op::Write { slot, .. } => *slot,
        Op::Read { slot, .. } => *slot,
        Op::Seek { .. } => return false,
        Op::Fstat { .. } => return false,
        Op::Stat { .. } => return false,
        Op::ReadDir => return false,
        Op::WriteStderr => return false,
        Op::Dup { .. } => return false,
        Op::Delete { .. } => return false,
    };

    let mut slots = [[None; FD_SLOTS]; WORKERS];
    for record in expected_records[..record_idx].iter() {
        match record {
            ComparableRecord::Case { .. } => {
                slots = [[None; FD_SLOTS]; WORKERS];
            }
            ComparableRecord::Result {
                worker, op, err, ..
            } => update_known_open_fds(&mut slots, *worker as usize, *op, *err),
            ComparableRecord::Snapshot { .. } | ComparableRecord::End { .. } => {}
        }
    }

    match slots[*expected_worker as usize][slot] {
        Some(KnownOpenFd {
            file: FileId::C,
            deleted_after_open: true,
        }) => true,
        Some(KnownOpenFd { .. }) => false,
        None => false,
    }
}

fn update_known_open_fds(
    slots: &mut [[Option<KnownOpenFd>; FD_SLOTS]; WORKERS],
    worker: usize,
    op: Op,
    err: OracleErr,
) {
    match op {
        Op::Open { slot, file, .. } => {
            if err == OracleErr::None {
                slots[worker][slot] = Some(KnownOpenFd {
                    file,
                    deleted_after_open: false,
                });
            }
        }
        Op::Close { slot } => {
            if err == OracleErr::None {
                slots[worker][slot] = None;
            }
        }
        Op::Write { .. } => {}
        Op::Read { .. } => {}
        Op::Seek { .. } => {}
        Op::Fstat { .. } => {}
        Op::Stat { .. } => {}
        Op::ReadDir => {}
        Op::WriteStderr => {}
        Op::Dup { src_slot, dst_slot } => {
            if err == OracleErr::None {
                slots[worker][dst_slot] = slots[worker][src_slot];
            }
        }
        Op::Delete { file } => {
            if file == FileId::C && err == OracleErr::None {
                for worker_slots in slots.iter_mut() {
                    for open in worker_slots.iter_mut().flatten() {
                        if open.file == FileId::C {
                            open.deleted_after_open = true;
                        }
                    }
                }
            }
        }
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
        ComparableRecord::Result { .. } | ComparableRecord::End { .. } => None,
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
            ComparableRecord::Result {
                worker,
                step,
                op,
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
                    "    -> step {} T{} {} rc={} err={:?}{}",
                    step,
                    worker + 1,
                    op.pseudocode(),
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
    let mut reader = ReportReader::open(path)?;
    let chain_len = reader.chain_len;
    let run = reader.run.clone();
    let mut records = Vec::new();
    let mut snapshots = Vec::new();

    while let Some((record, snapshot)) = reader.read_record()? {
        records.push(record);
        if let Some(snapshot) = snapshot {
            snapshots.push(snapshot);
        }
    }

    Ok(OracleReport {
        chain_len,
        run,
        records,
        snapshots,
    })
}

struct ReportReader {
    path: PathBuf,
    reader: BufReader<Box<dyn Read>>,
    chain_len: usize,
    run: String,
    current_case_id: u64,
    current_case_name: String,
    current_chain: Vec<Op>,
    current_steps: Vec<ScheduledStep>,
    current_step_idx: usize,
}

impl ReportReader {
    fn open(path: &Path) -> Result<Self, String> {
        let mut reader = BufReader::new(open_report_reader(path)?);
        let (chain_len, run) = read_report_header(&mut reader, path)?;
        Ok(ReportReader {
            path: path.to_path_buf(),
            reader,
            chain_len,
            run,
            current_case_id: 0,
            current_case_name: String::new(),
            current_chain: Vec::new(),
            current_steps: Vec::new(),
            current_step_idx: 0,
        })
    }

    fn read_record(&mut self) -> Result<Option<(ComparableRecord, Option<FileSnapshot>)>, String> {
        let tag = match read_record_tag(&mut self.reader)? {
            Some(tag) => tag,
            None => return Ok(None),
        };
        match tag {
            1 => self.read_case_record().map(Some),
            2 => self.read_result_record().map(Some),
            3 => self.read_snapshot_record().map(Some),
            4 => Ok(Some((
                ComparableRecord::End {
                    cases: read_var_u64(&mut self.reader)?,
                },
                None,
            ))),
            _ => Err(format!(
                "{} has invalid record tag {}",
                self.path.display(),
                tag
            )),
        }
    }

    fn read_case_record(&mut self) -> Result<(ComparableRecord, Option<FileSnapshot>), String> {
        let case_id = read_var_u64(&mut self.reader)?;
        let mask = read_var_u64(&mut self.reader)? as usize;
        let mut chain = Vec::with_capacity(self.chain_len);
        for _ in 0..self.chain_len {
            chain.push(Op::read_from(&mut self.reader)?);
        }
        self.current_case_id = case_id;
        self.current_case_name = format!("case-{case_id:06}");
        self.current_chain = chain.clone();
        self.current_steps = scheduled_steps(&chain, &Scheduler::new(WORKERS), mask);
        self.current_step_idx = 0;
        Ok((
            ComparableRecord::Case {
                case_id,
                mask: mask as u64,
            },
            None,
        ))
    }

    fn read_result_record(&mut self) -> Result<(ComparableRecord, Option<FileSnapshot>), String> {
        let step = self.current_step_idx + 1;
        let scheduled_step = self
            .current_steps
            .get(self.current_step_idx)
            .copied()
            .ok_or_else(|| {
                format!(
                    "{} has more result records than steps for case {}",
                    self.path.display(),
                    self.current_case_id
                )
            })?;
        self.current_step_idx += 1;
        let result = read_op_result(&mut self.reader)?;
        Ok((
            ComparableRecord::Result {
                step: step as u32,
                worker: scheduled_step.worker as u32,
                op: scheduled_step.op,
                rc: result.rc,
                err: result.err,
                data: result.data,
            },
            None,
        ))
    }

    fn read_snapshot_record(&mut self) -> Result<(ComparableRecord, Option<FileSnapshot>), String> {
        let file = FileId::from_tag(read_u8(&mut self.reader)?)?;
        let exists = read_u8(&mut self.reader)? != 0;
        let (len, hash) = if exists {
            (read_var_u64(&mut self.reader)?, read_u64(&mut self.reader)?)
        } else {
            (0, 0)
        };
        let rel = PathBuf::from(format!("{}/{}", self.current_case_name, file.name()));
        let snapshot = FileSnapshot {
            case_id: self.current_case_id as usize,
            file,
            rel: rel.clone(),
            exists,
            len,
            hash,
        };
        Ok((
            ComparableRecord::Snapshot {
                case_id: self.current_case_id,
                file,
                rel,
                exists,
                len,
                hash,
            },
            Some(snapshot),
        ))
    }
}

fn open_report_reader(path: &Path) -> Result<Box<dyn Read>, String> {
    let mut file =
        File::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    let mut magic = [0u8; 2];
    let bytes_read = file
        .read(&mut magic)
        .map_err(|e| format!("failed to read {} header: {e}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("failed to seek {}: {e}", path.display()))?;
    if bytes_read == 2 && magic == [0x1f, 0x8b] {
        Ok(Box::new(GzDecoder::new(file)))
    } else {
        Ok(Box::new(file))
    }
}

fn read_report_header<R: Read>(reader: &mut R, path: &Path) -> Result<(usize, String), String> {
    let mut magic = [0u8; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|e| format!("failed to read {} header: {e}", path.display()))?;
    if &magic != REPORT_MAGIC {
        return Err(format!("{} is not an oracle binary report", path.display()));
    }
    let version = read_u32(reader)?;
    if version != REPORT_VERSION {
        return Err(format!(
            "{} has unsupported oracle report version {}",
            path.display(),
            version
        ));
    }
    let chain_len = read_u64(reader)? as usize;
    let run = read_string(reader)?;
    Ok((chain_len, run))
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
    if is_bad_fd_error(e) {
        return OracleErr::BadFd;
    }

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
        _ => OracleErr::Other,
    }
}

fn is_bad_fd_error(e: &std::io::Error) -> bool {
    error_name_is_bad_fd(&e.to_string()) || bad_fd_errno(e.raw_os_error())
}

fn error_name_is_bad_fd(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.contains("bad file descriptor") || name.contains("badf")
}

fn bad_fd_errno(errno: Option<i32>) -> bool {
    match errno {
        Some(9) => true,
        #[cfg(target_family = "wasm")]
        Some(8) => true,
        _ => false,
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

fn write_var_u64<W: Write>(w: &mut W, mut value: u64) -> Result<(), String> {
    while value >= 0x80 {
        write_u8(w, (value as u8) | 0x80)?;
        value >>= 7;
    }
    write_u8(w, value as u8)
}

fn write_var_i64<W: Write>(w: &mut W, value: i64) -> Result<(), String> {
    write_var_u64(w, ((value << 1) ^ (value >> 63)) as u64)
}

fn write_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> Result<(), String> {
    write_u32(w, bytes.len() as u32)?;
    w.write_all(bytes)
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_string<W: Write>(w: &mut W, value: &str) -> Result<(), String> {
    write_bytes(w, value.as_bytes())
}

fn write_var_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> Result<(), String> {
    write_var_u64(w, bytes.len() as u64)?;
    w.write_all(bytes)
        .map_err(|e| format!("failed to write oracle report: {e}"))
}

fn write_op_result<W: Write>(w: &mut W, result: &OpResult) -> Result<(), String> {
    if result.err != OracleErr::None {
        write_u8(w, 4)?;
        return write_u8(w, result.err as u8);
    }
    if !result.data.is_empty() {
        write_u8(w, 3)?;
        return write_var_bytes(w, &result.data);
    }
    match result.rc {
        -1 => write_u8(w, 0),
        0 => write_u8(w, 1),
        rc => {
            write_u8(w, 2)?;
            write_var_i64(w, rc)
        }
    }
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

fn read_var_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let mut shift = 0;
    let mut value = 0u64;
    loop {
        let byte = read_u8(r)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err("invalid varint in oracle report".to_owned());
        }
    }
}

fn read_var_i64<R: Read>(r: &mut R) -> Result<i64, String> {
    let value = read_var_u64(r)?;
    Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)))
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

fn read_var_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, String> {
    let len = read_var_u64(r)? as usize;
    let mut bytes = vec![0u8; len];
    r.read_exact(&mut bytes)
        .map_err(|e| format!("failed to read oracle report: {e}"))?;
    Ok(bytes)
}

fn read_op_result<R: Read>(r: &mut R) -> Result<OpResult, String> {
    match read_u8(r)? {
        0 => Ok(OpResult::ok(-1)),
        1 => Ok(OpResult::ok(0)),
        2 => Ok(OpResult::ok(read_var_i64(r)?)),
        3 => {
            let data = read_var_bytes(r)?;
            Ok(OpResult::ok_data(data.len() as i64, data))
        }
        4 => Ok(OpResult::err(OracleErr::from_tag(read_u8(r)?)?)),
        tag => Err(format!("invalid op result tag {tag}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};

    fn generated_scheduled_case_count(len: usize) -> usize {
        let ops = concrete_ops();
        count_scheduled_suffix(0, len, &ops, CountState::default(), &mut HashMap::new())
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
    struct CountState {
        opened: [[bool; FD_SLOTS]; WORKERS],
        slot_maps: [[Option<usize>; FD_SLOTS]; WORKERS],
        file_map: [Option<usize>; 2],
        has_mutation: bool,
    }

    fn count_scheduled_suffix(
        pos: usize,
        len: usize,
        ops: &[Op],
        state: CountState,
        memo: &mut HashMap<(usize, CountState), usize>,
    ) -> usize {
        if pos == len {
            return usize::from(state.has_mutation);
        }
        if let Some(count) = memo.get(&(pos, state)) {
            return *count;
        }

        let workers: &[usize] = if pos == 0 { &[0] } else { &[0, 1] };
        let mut count = 0usize;
        for worker in workers {
            for op in ops {
                if let Some(next_state) = advance_count_state(state, *worker, *op) {
                    count += count_scheduled_suffix(pos + 1, len, ops, next_state, memo);
                }
            }
        }
        memo.insert((pos, state), count);
        count
    }

    fn advance_count_state(mut state: CountState, worker: usize, op: Op) -> Option<CountState> {
        if op.requires_fd() {
            if let Some(slot) = op.slot() {
                if !state.opened[worker][slot] {
                    return None;
                }
            }
        }

        match op {
            Op::Dup { src_slot, dst_slot } => {
                let canonical_src_slot = canonical_slot(&mut state.slot_maps[worker], src_slot);
                let canonical_dst_slot = canonical_slot(&mut state.slot_maps[worker], dst_slot);
                if canonical_src_slot != src_slot || canonical_dst_slot != dst_slot {
                    return None;
                }
            }
            _ => {
                if let Some(slot) = op.slot() {
                    let canonical_slot = canonical_slot(&mut state.slot_maps[worker], slot);
                    if canonical_slot != slot {
                        return None;
                    }
                }
            }
        }

        if let Some(file) = op.file() {
            if let Some(file_index) = file.canonicalizable_index() {
                let canonical_file_index = match state.file_map[file_index] {
                    Some(canonical_file_index) => canonical_file_index,
                    None => {
                        let canonical_file_index = state.file_map.iter().flatten().count();
                        state.file_map[file_index] = Some(canonical_file_index);
                        canonical_file_index
                    }
                };
                if canonical_file_index != file_index {
                    return None;
                }
            }
        }

        match op {
            Op::Open { slot, .. } => {
                state.opened[worker][slot] = true;
            }
            Op::Close { .. } => {}
            Op::Write { .. } => {}
            Op::Read { .. } => {}
            Op::Seek { .. } => {}
            Op::Fstat { .. } => {}
            Op::Stat { .. } => {}
            Op::ReadDir => {}
            Op::WriteStderr => {}
            Op::Dup { dst_slot, .. } => {
                state.opened[worker][dst_slot] = true;
            }
            Op::Delete { .. } => {}
        }
        state.has_mutation |= op.is_mutation();
        Some(state)
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

    fn no_progress(_: &OracleProgress) -> Result<(), String> {
        Ok(())
    }

    fn test_fixture_root(root: &Path) -> PathBuf {
        let fixtures = root.join("fixtures");
        prepare_theory_trial_fixtures(&fixtures).unwrap();
        fixtures
    }

    #[test]
    fn generator_expands_all_operation_and_worker_permutations() {
        assert_eq!(FILES.len(), 3);
        assert_eq!(SNAPSHOT_FILES.len(), 2);
        assert_eq!(FIXTURE_FILES.len(), 2);
        assert_eq!(concrete_ops().len(), 18);
    }

    #[test]
    fn final_scheduled_case_counts_by_chain_length() {
        let counts: Vec<usize> = (1..=5).map(generated_scheduled_case_count).collect();
        assert_eq!(counts, vec![4, 72, 1_636, 43_248, 1_261_228]);
    }

    #[test]
    fn oracle_shard_report_compares_against_full_report() {
        let root = temp_oracle_dir("shard-report");
        let full_root = root.join("full-run");
        let shard_root = root.join("shard-run");
        fs::create_dir_all(&full_root).unwrap();
        fs::create_dir_all(&shard_root).unwrap();
        let full_fixtures = test_fixture_root(&full_root);
        let shard_fixtures = test_fixture_root(&shard_root);
        let full_report = root.join("full.bin.gz");
        let shard_report = root.join("shard.bin");
        let mut progress = no_progress;

        run_suite(
            &full_root,
            &full_fixtures,
            1,
            &full_report,
            OracleSelection::Exhaustive(OracleShard {
                start: 1,
                count: None,
            }),
            &mut progress,
        )
        .unwrap();
        run_suite(
            &shard_root,
            &shard_fixtures,
            1,
            &shard_report,
            OracleSelection::Exhaustive(OracleShard {
                start: 2,
                count: Some(3),
            }),
            &mut progress,
        )
        .unwrap();

        compare_reports(&full_report, &shard_report).unwrap();

        let shard = read_report(&shard_report).unwrap();
        assert_eq!(report_case_ids(&shard.records), BTreeSet::from([2, 3, 4]));
        assert!(matches!(
            shard.records.last(),
            Some(ComparableRecord::End { cases: 4 })
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn oracle_sample_report_compares_against_full_report() {
        let root = temp_oracle_dir("sample-report");
        let full_root = root.join("full-run");
        let sample_root = root.join("sample-run");
        fs::create_dir_all(&full_root).unwrap();
        fs::create_dir_all(&sample_root).unwrap();
        let full_fixtures = test_fixture_root(&full_root);
        let sample_fixtures = test_fixture_root(&sample_root);
        let full_report = root.join("full.bin.gz");
        let sample_report = root.join("sample.bin");
        let mut progress = no_progress;

        run_suite(
            &full_root,
            &full_fixtures,
            2,
            &full_report,
            OracleSelection::Exhaustive(OracleShard {
                start: 1,
                count: None,
            }),
            &mut progress,
        )
        .unwrap();
        run_suite(
            &sample_root,
            &sample_fixtures,
            2,
            &sample_report,
            OracleSelection::Sample { count: 7, seed: 7 },
            &mut progress,
        )
        .unwrap();

        compare_reports(&full_report, &sample_report).unwrap();

        let sample = read_report(&sample_report).unwrap();
        assert_eq!(report_case_ids(&sample.records).len(), 7);
        assert!(matches!(
            sample.records.last(),
            Some(ComparableRecord::End { cases: 72 })
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn streamed_sample_report_compares_against_full_report() {
        let root = temp_oracle_dir("streamed-sample-report");
        let full_root = root.join("full-run");
        let sample_root = root.join("sample-run");
        fs::create_dir_all(&full_root).unwrap();
        fs::create_dir_all(&sample_root).unwrap();
        let full_fixtures = test_fixture_root(&full_root);
        let sample_fixtures = test_fixture_root(&sample_root);
        let full_report = root.join("full.bin.gz");
        let sample_report = root.join("sample.bin");
        let mut progress = no_progress;

        run_suite(
            &full_root,
            &full_fixtures,
            2,
            &full_report,
            OracleSelection::Exhaustive(OracleShard {
                start: 1,
                count: None,
            }),
            &mut progress,
        )
        .unwrap();
        run_sample_from_expected(
            &sample_root,
            &sample_fixtures,
            2,
            &sample_report,
            &full_report,
            20,
            100,
            &mut progress,
        )
        .unwrap();

        compare_reports(&full_report, &sample_report).unwrap();

        let sample = read_report(&sample_report).unwrap();
        let selected = report_case_ids(&sample.records).len();
        assert!(selected > 0 && selected < 72);
        assert!(matches!(
            sample.records.last(),
            Some(ComparableRecord::End { cases: 72 })
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn oracle_sample_ids_are_deterministic_and_spread() {
        let first = sample_case_ids(1000, 20, 1234).unwrap();
        let second = sample_case_ids(1000, 20, 1234).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 20);
        let min = first.iter().next().copied().unwrap();
        let max = first.iter().next_back().copied().unwrap();
        assert!(max - min > 500);
    }

    #[test]
    fn oracle_sample_ids_change_with_seed() {
        let first = sample_case_ids(1000, 20, 1234).unwrap();
        let second = sample_case_ids(1000, 20, 5678).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn theory_trial_fixture_content_is_deterministic_from_name() {
        let first = theory_trial_1_fixture_bytes("file123");
        let second = theory_trial_1_fixture_bytes("file123");
        let other = theory_trial_1_fixture_bytes("file124");
        assert_eq!(first, second);
        assert_ne!(first, other);
        assert!((1..=1024).contains(&first.len()));
        assert!(!first.is_empty());
    }

    #[test]
    fn theory_trial_fixtures_are_generated_for_oracle_ops() {
        let root = temp_oracle_dir("theory-trial-1");
        let fixtures = root.join("fixtures");
        prepare_theory_trial_fixtures(&fixtures).unwrap();
        assert_eq!(
            fs::read(fixtures.join("file1")).unwrap(),
            theory_trial_1_fixture_bytes("file1")
        );
        assert!(fixture_path(&fixtures, 1, FileId::Fixture1).exists());
        assert!(fixture_path(&fixtures, 1, FileId::Fixture2).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_fixture_file_maps_directly_from_run_fixture_id() {
        let root = Path::new("/fixtures");
        assert_eq!(
            fixture_path(root, 1, FileId::Fixture1),
            PathBuf::from("/fixtures/file1")
        );
        assert_eq!(
            fixture_path(root, 20_000, FileId::Fixture1),
            PathBuf::from("/fixtures/file20000")
        );
        assert_eq!(selected_run_fixture_id(1_636, 123, 7), 123);
        assert_eq!(selected_run_fixture_id(1_261_228, 123_456, 7), 7);
    }

    #[test]
    fn active_fixture_file_rejects_runs_larger_than_fixture_pool() {
        let ok = OracleCaseSelection::Range {
            start: 1_000_000,
            end: 1_000_000 + THEORY_TRIAL_1_FIXTURE_COUNT,
            count: THEORY_TRIAL_1_FIXTURE_COUNT,
        };
        assert!(ensure_selected_fixtures_available(&ok).is_ok());

        let too_many = OracleCaseSelection::Range {
            start: 1,
            end: THEORY_TRIAL_1_FIXTURE_COUNT + 2,
            count: THEORY_TRIAL_1_FIXTURE_COUNT + 1,
        };
        assert!(ensure_selected_fixtures_available(&too_many)
            .unwrap_err()
            .contains("exceeds fixture pool size"));
    }

    #[test]
    fn fixture_limit_is_not_enforced_for_full_expected_report_generation() {
        assert!(!OracleSelection::Exhaustive(OracleShard {
            start: 1,
            count: None,
        })
        .enforces_runtime_fixture_limit());
        assert!(OracleSelection::Exhaustive(OracleShard {
            start: 1,
            count: Some(THEORY_TRIAL_1_FIXTURE_COUNT + 1),
        })
        .enforces_runtime_fixture_limit());
        assert!(OracleSelection::Sample {
            count: THEORY_TRIAL_1_FIXTURE_COUNT + 1,
            seed: 1,
        }
        .enforces_runtime_fixture_limit());
    }

    fn rec(step: u32, worker: usize, op: Op, rc: i64, err: OracleErr) -> ComparableRecord {
        ComparableRecord::Result {
            step,
            worker: worker as u32,
            op,
            rc,
            err,
            data: Vec::new(),
        }
    }

    fn case_records(steps: &[(usize, Op, i64, OracleErr)]) -> Vec<ComparableRecord> {
        let mut records = vec![ComparableRecord::Case {
            case_id: 1,
            mask: 0,
        }];
        for (idx, (worker, op, rc, err)) in steps.iter().copied().enumerate() {
            records.push(rec((idx + 1) as u32, worker, op, rc, err));
        }
        records
    }

    fn actual_not_found(record: &ComparableRecord) -> ComparableRecord {
        let ComparableRecord::Result {
            step, worker, op, ..
        } = record
        else {
            panic!("expected result record");
        };
        rec(*step, *worker as usize, *op, -1, OracleErr::NotFound)
    }

    fn assert_known_tmp_unlink_ignored(steps: &[(usize, Op, i64, OracleErr)], fail_step: usize) {
        let records = case_records(steps);
        let idx = fail_step;
        let actual = actual_not_found(&records[idx]);
        assert!(
            ignore_known_error(&records, idx, &records[idx], &actual),
            "expected step {fail_step} to be ignored"
        );
    }

    fn assert_known_tmp_unlink_not_ignored(
        steps: &[(usize, Op, i64, OracleErr)],
        fail_step: usize,
        actual: ComparableRecord,
    ) {
        let records = case_records(steps);
        let idx = fail_step;
        assert!(
            !ignore_known_error(&records, idx, &records[idx], &actual),
            "expected step {fail_step} to remain reportable"
        );
    }

    fn delete(file: FileId) -> Op {
        Op::Delete { file }
    }

    fn open(slot: usize, file: FileId, mode: OpenMode) -> Op {
        Op::Open { slot, file, mode }
    }

    fn read(slot: usize, size: ReadSize) -> Op {
        Op::Read { slot, size }
    }

    fn write(slot: usize, payload: WritePayload) -> Op {
        Op::Write { slot, payload }
    }

    #[test]
    fn verifier_ignores_only_known_tmp_open_unlink_read_write_failures() {
        use FileId::{A, B, C};
        use OpenMode::{AppendCreate, ReadWriteCreate};
        use OracleErr::{None, NotFound};
        use ReadSize::{Bytes32 as Read32, Bytes4K as Read4K};
        use WritePayload::{Bytes32 as Write32, Bytes32K as Write32K, Bytes4K as Write4K};

        let cases = [
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, delete(B), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (0, delete(C), -1, None),
                    (1, write(0, Write32), 32, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (0, open(0, A, AppendCreate), -1, None),
                    (0, open(0, C, AppendCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, read(0, Read4K), 0, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, C, AppendCreate), -1, None),
                    (0, delete(C), -1, None),
                    (1, delete(C), -1, NotFound),
                    (1, read(0, Read4K), 0, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (0, open(0, C, AppendCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, delete(C), -1, NotFound),
                    (0, write(0, Write32K), 32768, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (0, open(0, C, AppendCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, open(1, B, AppendCreate), -1, None),
                    (0, write(0, Write32), 32, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, A, ReadWriteCreate), -1, None),
                    (1, open(0, C, AppendCreate), -1, None),
                    (0, delete(C), -1, None),
                    (1, write(0, Write32), 32, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(B), -1, NotFound),
                    (0, delete(C), -1, None),
                    (1, read(0, Read4K), 0, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (0, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, read(0, Read32), 0, None),
                    (0, delete(B), -1, NotFound),
                ],
                4,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (1, read(0, Read4K), 0, None),
                    (1, delete(C), -1, NotFound),
                ],
                4,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (0, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, write(0, Write4K), 4096, None),
                    (0, read(0, Read32), 0, None),
                ],
                4,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (0, delete(C), -1, None),
                    (1, Op::WriteStderr, 77, None),
                    (1, write(0, Write32K), 32768, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(A), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (1, Op::WriteStderr, 77, None),
                    (0, delete(C), -1, None),
                    (1, write(0, Write4K), 4096, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(C), -1, None),
                    (0, open(0, C, AppendCreate), -1, None),
                    (0, delete(C), -1, None),
                    (0, write(0, Write32K), 32768, None),
                    (1, delete(A), -1, NotFound),
                ],
                4,
            ),
            (
                vec![
                    (0, delete(C), -1, None),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (0, delete(C), -1, None),
                    (0, open(0, A, ReadWriteCreate), -1, None),
                    (1, read(0, Read32), 0, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(C), -1, None),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, open(0, A, ReadWriteCreate), -1, None),
                    (1, write(0, Write32), 32, None),
                ],
                5,
            ),
            (
                vec![
                    (0, delete(C), -1, None),
                    (0, open(0, C, ReadWriteCreate), -1, None),
                    (0, delete(C), -1, None),
                    (0, write(0, Write32), 32, None),
                    (0, Op::WriteStderr, 77, None),
                ],
                4,
            ),
            (
                vec![
                    (0, delete(C), -1, None),
                    (0, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (0, write(0, Write32K), 32768, None),
                    (0, open(0, C, ReadWriteCreate), -1, None),
                ],
                4,
            ),
            (
                vec![
                    (0, open(0, A, AppendCreate), -1, None),
                    (1, delete(C), -1, NotFound),
                    (1, open(0, C, ReadWriteCreate), -1, None),
                    (1, delete(C), -1, None),
                    (1, write(0, Write4K), 4096, None),
                ],
                5,
            ),
            (
                vec![
                    (0, open(0, A, AppendCreate), -1, None),
                    (0, delete(C), -1, NotFound),
                    (0, open(1, C, ReadWriteCreate), -1, None),
                    (0, delete(C), -1, None),
                    (0, write(1, Write32), 32, None),
                ],
                5,
            ),
        ];

        for (steps, fail_step) in cases {
            assert_known_tmp_unlink_ignored(&steps, fail_step);
        }
    }

    #[test]
    fn verifier_does_not_ignore_near_misses() {
        use FileId::{A, C};
        use OpenMode::ReadWriteCreate;
        use OracleErr::{BadFd, None, NotFound};
        use ReadSize::Bytes32 as Read32;
        use WritePayload::Bytes32 as Write32;

        let persistent_file = vec![
            (0, open(0, A, ReadWriteCreate), -1, None),
            (0, delete(A), -1, None),
            (0, write(0, Write32), 32, None),
        ];
        assert_known_tmp_unlink_not_ignored(
            &persistent_file,
            3,
            actual_not_found(&case_records(&persistent_file)[3]),
        );

        let failed_delete = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, NotFound),
            (0, write(0, Write32), 32, None),
        ];
        assert_known_tmp_unlink_not_ignored(
            &failed_delete,
            3,
            actual_not_found(&case_records(&failed_delete)[3]),
        );

        let closed_handle = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, None),
            (0, Op::Close { slot: 0 }, -1, None),
            (0, write(0, Write32), -1, BadFd),
        ];
        assert_known_tmp_unlink_not_ignored(
            &closed_handle,
            4,
            actual_not_found(&case_records(&closed_handle)[4]),
        );

        let reopened_slot = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, None),
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, write(0, Write32), 32, None),
        ];
        assert_known_tmp_unlink_not_ignored(
            &reopened_slot,
            4,
            actual_not_found(&case_records(&reopened_slot)[4]),
        );

        let stderr_mismatch = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, None),
            (0, Op::WriteStderr, 77, None),
        ];
        assert_known_tmp_unlink_not_ignored(
            &stderr_mismatch,
            3,
            actual_not_found(&case_records(&stderr_mismatch)[3]),
        );

        let expected_error = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, None),
            (0, read(0, Read32), -1, BadFd),
        ];
        assert_known_tmp_unlink_not_ignored(
            &expected_error,
            3,
            actual_not_found(&case_records(&expected_error)[3]),
        );

        let actual_bad_fd = vec![
            (0, open(0, C, ReadWriteCreate), -1, None),
            (0, delete(C), -1, None),
            (0, write(0, Write32), 32, None),
        ];
        assert_known_tmp_unlink_not_ignored(
            &actual_bad_fd,
            3,
            rec(3, 0, write(0, Write32), -1, BadFd),
        );
    }

    #[test]
    fn read_dir_listing_ignores_nfs_tombstones() {
        let root = temp_oracle_dir("nfs-dir-entry");
        fs::write(root.join("A"), b"a").unwrap();
        fs::write(root.join(".nfs123456"), b"temporary nfs tombstone").unwrap();

        let listing = String::from_utf8(read_dir_listing(&root).unwrap()).unwrap();
        assert_eq!(listing, "A");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_mutation_classification_is_explicit() {
        assert!(Op::Open {
            slot: 0,
            file: FileId::A,
            mode: OpenMode::ReadWriteCreate,
        }
        .is_mutation());
        assert!(Op::Open {
            slot: 0,
            file: FileId::A,
            mode: OpenMode::AppendCreate,
        }
        .is_mutation());
        assert!(Op::Open {
            slot: 0,
            file: FileId::A,
            mode: OpenMode::ReadWriteTruncate,
        }
        .is_mutation());
        assert!(Op::Open {
            slot: 0,
            file: FileId::A,
            mode: OpenMode::ReadWriteCreateNew,
        }
        .is_mutation());
        assert!(Op::Write {
            slot: 0,
            payload: WritePayload::Bytes32,
        }
        .is_mutation());
        assert!(Op::Delete { file: FileId::A }.is_mutation());
        assert!(Op::Dup {
            src_slot: 0,
            dst_slot: 1,
        }
        .is_mutation());

        assert!(!Op::Open {
            slot: 0,
            file: FileId::Fixture1,
            mode: OpenMode::ReadOnlyExisting,
        }
        .is_mutation());
        assert!(!Op::Close { slot: 0 }.is_mutation());
        assert!(!Op::Read {
            slot: 0,
            size: ReadSize::Bytes32,
        }
        .is_mutation());
        assert!(!Op::Seek {
            slot: 0,
            target: SeekTarget::Start,
        }
        .is_mutation());
        assert!(!Op::Fstat { slot: 0 }.is_mutation());
        assert!(!Op::Stat { file: FileId::A }.is_mutation());
        assert!(!Op::ReadDir.is_mutation());
        assert!(!Op::WriteStderr.is_mutation());
    }

    #[test]
    fn operation_fd_requirement_classification_is_explicit() {
        assert!(Op::Close { slot: 0 }.requires_fd());
        assert!(Op::Write {
            slot: 0,
            payload: WritePayload::Bytes32,
        }
        .requires_fd());
        assert!(Op::Read {
            slot: 0,
            size: ReadSize::Bytes32,
        }
        .requires_fd());
        assert!(Op::Seek {
            slot: 0,
            target: SeekTarget::Start,
        }
        .requires_fd());
        assert!(Op::Fstat { slot: 0 }.requires_fd());
        assert!(Op::Dup {
            src_slot: 0,
            dst_slot: 1,
        }
        .requires_fd());

        assert!(!Op::Open {
            slot: 0,
            file: FileId::A,
            mode: OpenMode::ReadWriteCreate,
        }
        .requires_fd());
        assert!(!Op::Open {
            slot: 0,
            file: FileId::Fixture1,
            mode: OpenMode::ReadOnlyExisting,
        }
        .requires_fd());
        assert!(!Op::Stat { file: FileId::A }.requires_fd());
        assert!(!Op::Delete { file: FileId::A }.requires_fd());
        assert!(!Op::ReadDir.requires_fd());
        assert!(!Op::WriteStderr.requires_fd());
    }

    #[test]
    fn operation_spec_expansion_metadata_is_explicit() {
        let cases = [
            (OpSpec::Open(OpenMode::ReadWriteCreate), true, true),
            (OpSpec::Open(OpenMode::ReadOnlyExisting), true, true),
            (OpSpec::Close, false, true),
            (OpSpec::Write(WritePayload::Bytes32), false, true),
            (OpSpec::Read(ReadSize::Bytes32), false, true),
            (OpSpec::Seek(SeekTarget::Start), false, true),
            (OpSpec::Fstat, false, true),
            (OpSpec::Stat, true, false),
            (OpSpec::ReadDir, false, false),
            (OpSpec::WriteStderr, false, false),
            (OpSpec::Dup, false, true),
            (OpSpec::Delete, true, false),
        ];

        for (spec, needs_file, needs_fd) in cases {
            assert_eq!(spec.needs_file(), needs_file, "{spec:?}");
            assert_eq!(spec.needs_fd(), needs_fd, "{spec:?}");
        }
    }

    #[test]
    fn bad_fd_errors_are_canonicalized_by_name_not_only_errno() {
        let host_bad_fd = std::io::Error::from_raw_os_error(9);
        assert_eq!(errcode(&host_bad_fd), OracleErr::BadFd);

        let wasix_bad_fd = std::io::Error::new(std::io::ErrorKind::Other, "Bad file descriptor");
        assert_eq!(errcode(&wasix_bad_fd), OracleErr::BadFd);

        let wasi_badf_name = std::io::Error::new(std::io::ErrorKind::Other, "BADF");
        assert_eq!(errcode(&wasi_badf_name), OracleErr::BadFd);

        #[cfg(not(target_family = "wasm"))]
        assert_ne!(
            errcode(&std::io::Error::from_raw_os_error(8)),
            OracleErr::BadFd
        );
    }

    #[test]
    fn non_mutating_chains_are_filtered() {
        assert!(!chain_is_enabled(&[
            Op::Close { slot: 0 },
            Op::Read {
                slot: 0,
                size: ReadSize::Bytes32,
            },
        ]));
        assert!(chain_is_enabled(&[
            Op::Close { slot: 0 },
            Op::Write {
                slot: 0,
                payload: WritePayload::Bytes32,
            },
        ]));
    }

    #[test]
    fn scheduled_case_normalization_filters_fd_ops_before_open() {
        let opened_then_stale_write = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Close { slot: 0 },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert_eq!(
            normalize_scheduled_case(&opened_then_stale_write),
            opened_then_stale_write
        );
        assert!(scheduled_case_is_enabled(&opened_then_stale_write));

        let never_opened_write = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Close { slot: 0 },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Close { slot: 0 },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert!(normalize_scheduled_case(&never_opened_write).is_empty());
        assert!(!scheduled_case_is_enabled(&never_opened_write));
    }

    #[test]
    fn scheduled_case_normalization_canonicalizes_worker_fd_slots() {
        let canonical = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert_eq!(normalize_scheduled_case(&canonical), canonical);
        assert!(scheduled_case_is_enabled(&canonical));

        let renamed_duplicate = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 1,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 1,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert_eq!(normalize_scheduled_case(&renamed_duplicate), canonical);
        assert!(!scheduled_case_is_enabled(&renamed_duplicate));

        let per_worker_slots_are_independent = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 1,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
        ];
        assert!(scheduled_case_is_enabled(&per_worker_slots_are_independent));
    }

    #[test]
    fn scheduled_case_normalization_canonicalizes_persistent_files() {
        let canonical = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert!(scheduled_case_is_enabled(&canonical));

        let renamed_duplicate = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::B,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Write {
                    slot: 0,
                    payload: WritePayload::Bytes32,
                },
            },
        ];
        assert_eq!(normalize_scheduled_case(&renamed_duplicate), canonical);
        assert!(!scheduled_case_is_enabled(&renamed_duplicate));

        let swapped = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::B,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 1,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
        ];
        let canonical_swapped = vec![
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 0,
                    file: FileId::A,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
            ScheduledStep {
                worker: 0,
                op: Op::Open {
                    slot: 1,
                    file: FileId::B,
                    mode: OpenMode::ReadWriteCreate,
                },
            },
        ];
        assert_eq!(normalize_scheduled_case(&swapped), canonical_swapped);
        assert!(!scheduled_case_is_enabled(&swapped));

        let tmp_file_is_not_canonicalized = vec![ScheduledStep {
            worker: 0,
            op: Op::Open {
                slot: 0,
                file: FileId::C,
                mode: OpenMode::ReadWriteCreate,
            },
        }];
        assert_eq!(
            normalize_scheduled_case(&tmp_file_is_not_canonicalized),
            tmp_file_is_not_canonicalized
        );
        assert!(scheduled_case_is_enabled(&tmp_file_is_not_canonicalized));
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
        let fixture_root = Arc::new(test_fixture_root(root.as_ref()));
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
                fixture_id: 1,
                fixture_root: Arc::clone(&fixture_root),
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
                fixture_id: 1,
                fixture_root: Arc::clone(&fixture_root),
            },
        );
        let t1_close = execute_job(
            0,
            &state,
            Job {
                op: Op::Close { slot: 0 },
                case_root: Arc::clone(&root),
                fixture_id: 1,
                fixture_root: Arc::clone(&fixture_root),
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
                fixture_id: 1,
                fixture_root: Arc::clone(&fixture_root),
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
                fixture_id: 1,
                fixture_root: Arc::clone(&fixture_root),
            },
        );

        assert_eq!(t1_open.result.err, OracleErr::None);
        assert_eq!(t2_open.result.err, OracleErr::None);
        assert_eq!(t1_close.result.err, OracleErr::None);
        assert_eq!(t1_write_after_close.result.err, OracleErr::BadFd);
        assert_eq!(t2_write_after_t1_close.result, OpResult::ok(32));
        assert!(matches!(
            state.lock().unwrap().workers[0].slots[0],
            FdSlot::Closed(_)
        ));

        let _ = fs::remove_dir_all(root.as_ref());
    }
}

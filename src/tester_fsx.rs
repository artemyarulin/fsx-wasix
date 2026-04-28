// vim: tw=80
use std::{
    ffi::OsString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    num::{NonZeroU64, NonZeroUsize},
    os::fd::IntoRawFd,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process, thread,
    time::{Duration, Instant},
};

use log::{debug, error, info, log, warn, Level};
use rand::{
    distributions::{Distribution, WeightedIndex},
    thread_rng, Rng, RngCore, SeedableRng,
};
use rand_xorshift::XorShiftRng;
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferWrite};
use serde_derive::Deserialize;

use crate::Cli;

/// Calculate the maximum field width needed to print numbers up to this size
fn field_width(max: usize, hex: bool) -> usize {
    if hex {
        2 + (8 * mem::size_of_val(&max) - max.leading_zeros() as usize)
            .div_ceil(4)
    } else {
        1 + (max as f64).log(10.0) as usize
    }
}
const fn default_flen() -> u64 {
    256 * 1024
}

/// Configuration file format, as toml
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct Config {
    /// Maximum file size
    // NB: could be u64, but the C-based FSX only works with 32-bit file sizes
    #[serde(default)]
    pub(crate) flen: Option<u32>,

    /// Disable verifications of file size
    #[serde(default)]
    pub(crate) nosizechecks: bool,

    /// Specifies size distribution for all operations
    #[serde(default)]
    pub(crate) opsize: Opsize,

    /// Specifies relative statistical weights of all operations
    #[serde(default)]
    pub(crate) weights: Weights,
}

impl Config {
    pub(crate) fn load(path: &PathBuf) -> Self {
        let r = match fs::read_to_string(path) {
            Ok(s) => toml::from_str(&s),
            Err(e) => {
                eprintln!("Error reading config file: {e}");
                process::exit(1);
            }
        };
        match r {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error reading config file: {e}");
                process::exit(1);
            }
        }
    }

    /// Validate compatibility with these CLI arguments
    pub(crate) fn validate_result(&self, _cli: &Cli) -> Result<(), String> {
        if self.flen == Some(0) {
            return Err("file length must be greater than zero".to_owned());
        }
        if self.opsize.max == 0 {
            return Err(
                "Maximum operation size must be greater than zero".to_owned()
            );
        }
        if self.opsize.min > self.opsize.max {
            return Err(
                "Minimum operation size must be no greater than maximum"
                    .to_owned(),
            );
        }
        let align = self.opsize.align.map(usize::from).unwrap_or(1);
        if align > self.opsize.max {
            return Err(
                "operation alignment must be no greater than maximum operation size"
                    .to_owned(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate(&self, cli: &Cli) {
        if let Err(e) = self.validate_result(cli) {
            eprintln!("error: {e}");
            process::exit(2);
        }
    }
}

const fn default_opsize_max() -> usize {
    65536
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub(crate) struct Opsize {
    /// Minium size for operations
    #[serde(default)]
    pub(crate) min: usize,
    /// Maximum size for operations
    #[serde(default = "default_opsize_max")]
    pub(crate) max: usize,
    /// Alignment in bytes for all operations
    pub(crate) align: Option<NonZeroUsize>,
}

impl Default for Opsize {
    fn default() -> Self {
        Opsize {
            min: 0,
            max: 65536,
            align: NonZeroUsize::new(1),
        }
    }
}

const fn default_weight() -> f64 {
    10.0
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Weights {
    #[serde(default)]
    pub(crate) close_open: f64,
    #[serde(default = "default_weight")]
    pub(crate) mapread: f64,
    #[serde(default = "default_weight")]
    pub(crate) mapwrite: f64,
    #[serde(default = "default_weight")]
    pub(crate) read: f64,
    #[serde(default = "default_weight")]
    pub(crate) write: f64,
    #[serde(default = "default_weight")]
    pub(crate) truncate: f64,
    #[serde(default)]
    pub(crate) fsync: f64,
    #[serde(default)]
    pub(crate) fdatasync: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights {
            close_open: 1.0,
            mapread: 1.0,
            mapwrite: 1.0,
            read: 1.0,
            write: 1.0,
            truncate: 1.0,
            fsync: 0.0,
            fdatasync: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Op {
    CloseOpen,
    Read,
    Write,
    MapRead,
    Truncate,
    MapWrite,
    Fsync,
    Fdatasync,
}

impl Op {
    fn make_weighted_index<I>(weights: I) -> WeightedIndex<f64>
    where
        I: IntoIterator<Item = f64> + ExactSizeIterator,
    {
        assert_eq!(weights.len(), 8);
        WeightedIndex::new(weights).unwrap()
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Op::CloseOpen => "close/open".fmt(f),
            Op::Read => "read".fmt(f),
            Op::Write => "write".fmt(f),
            Op::MapRead => "mapread".fmt(f),
            Op::Truncate => "truncate".fmt(f),
            Op::MapWrite => "mapwrite".fmt(f),
            Op::Fsync => "fsync".fmt(f),
            Op::Fdatasync => "fdatasync".fmt(f),
        }
    }
}

impl Distribution<Op> for WeightedIndex<f64> {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Op {
        match self.sample(rng) {
            0usize => Op::CloseOpen,
            1 => Op::Read,
            2 => Op::Write,
            3 => Op::MapRead,
            4 => Op::Truncate,
            5 => Op::MapWrite,
            6 => Op::Fsync,
            7 => Op::Fdatasync,
            _ => panic!("WeightedIndex was generated with too many keys"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum LogEntry {
    Skip(Op),
    CloseOpen,
    // offset, size
    Read(u64, usize),
    // old file len, offset, size
    Write(u64, u64, usize),
    // offset, size
    MapRead(u64, usize),
    // old file len, new file len
    Truncate(u64, u64),
    // old file len, offset, size
    MapWrite(u64, u64, usize),
    Fsync,
    Fdatasync,
}

#[derive(Debug)]
struct FsxError {
    message: String,
    report: String,
}

impl fmt::Display for FsxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.report.is_empty() {
            self.message.fmt(f)
        } else {
            write!(f, "{}\n{}", self.message, self.report)
        }
    }
}

type FsxResult<T = ()> = Result<T, FsxError>;

struct Exerciser {
    align: usize,
    artifacts_dir: Option<PathBuf>,
    /// Current file size
    file_size: u64,
    flen: u64,
    fname: PathBuf,
    /// Width for printing fields containing file offsets
    fwidth: usize,
    /// Inject an error on this step
    inject: Option<u64>,
    // What the file ought to contain
    good_buf: Vec<u8>,
    /// Monitor these byte ranges in extra detail.
    monitor: Option<(u64, u64)>,
    nosizechecks: bool,
    numops: Option<u64>,
    // Records most recent operations for future dumping
    oplog: AllocRingBuffer<LogEntry>,
    opsize: Opsize,
    seed: u64,
    // 0-indexed operation number to begin real transfers.
    simulatedopcount: u64,
    /// Width for printing fields containing operation sizes
    swidth: usize,
    /// Width for printing the step number field
    stepwidth: usize,
    // File's original data
    original_buf: Vec<u8>,
    // Use XorShiftRng because it's deterministic and seedable
    rng: XorShiftRng,
    // Number of steps completed so far
    steps: u64,
    file: File,
    wi: WeightedIndex<f64>,
}

impl Exerciser {
    fn check_buffers(&self, buf: &[u8], mut offset: u64) -> FsxResult {
        let mut size = buf.len();
        if self.good_buf[offset as usize..offset as usize + size] != buf[..] {
            error!("miscompare: offset= {offset:#x}, size = {size:#x}");
            let mut i = 0;
            let mut n = 0;
            let mut good = 0;
            let mut bad = 0;
            let mut badoffset = 0;
            let mut op = 0;
            error!(
                "{:fwidth$} GOOD  BAD  {:swidth$}",
                "OFFSET",
                "RANGE",
                fwidth = self.fwidth,
                swidth = self.swidth
            );
            while size > 0 {
                let c = self.good_buf[offset as usize];
                let t = buf[i];
                if c != t {
                    if n == 0 {
                        good = c;
                        bad = t;
                        badoffset = offset;
                        op = buf[if offset & 1 != 0 { i + 1 } else { i }];
                    }
                    n += 1;
                }
                offset += 1;
                i += 1;
                size -= 1;
            }
            assert!(n > 0);
            // XXX The reported range may be a little too small, because
            // some bytes in the damaged range may coincidentally match.  But
            // this is the way that the C-based FSX reported it.
            error!(
                "{:#fwidth$x} {:#04x} {:#04x} {:#swidth$x}",
                badoffset,
                good,
                bad,
                n,
                fwidth = self.fwidth,
                swidth = self.swidth
            );
            if op > 0 {
                error!("Step# (mod 256) for a misdirected write may be {op}");
            } else {
                error!(
                    "Step# for the bad data is unknown; check HOLE and EXTEND \
                     ops"
                );
            }
            return Err(self.fail("miscompare"));
        }
        Ok(())
    }

    fn check_size(&mut self) -> FsxResult {
        if !self.nosizechecks {
            let size = self.file.metadata().unwrap().len();
            if size != self.file_size {
                error!(
                    "Size error: expected {:#x} but found {:#x} by stat",
                    self.file_size, size
                );
                return Err(self.fail("size check failed"));
            }
        }
        Ok(())
    }

    /// Close and reopen the file
    fn closeopen(&mut self) {
        self.oplog.push(LogEntry::CloseOpen);

        if self.skip() {
            return;
        }
        info!("{:width$} close/open", self.steps, width = self.stepwidth);

        // We must remove and drop the old File before opening it, and that
        // requires swapping its contents.
        // Safe because we never access the uninitialized File object.
        unsafe {
            let placeholder: File = mem::MaybeUninit::zeroed().assume_init();
            drop(mem::replace(&mut self.file, placeholder));
            let newfile = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.fname)
                .expect("Cannot open file");
            let placeholder = mem::replace(&mut self.file, newfile);
            let _ = placeholder.into_raw_fd();
        }
    }

    fn doread(
        &mut self,
        buf: &mut [u8],
        offset: u64,
        size: usize,
    ) -> FsxResult {
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        let read = self.file.read(buf).unwrap();
        if read < size {
            error!("short read: {read:#x} bytes instead of {size:#x}");
            return Err(self.fail("short read"));
        }
        Ok(())
    }

    fn domapread(
        &mut self,
        buf: &mut [u8],
        offset: u64,
        size: usize,
    ) -> FsxResult {
        // Keep mapread in the operation mix using positioned I/O.
        self.doread(buf, offset, size)
    }

    fn domapwrite(
        &mut self,
        cur_file_size: u64,
        size: usize,
        offset: u64,
    ) -> FsxResult {
        self.dowrite(cur_file_size, size, offset)
    }

    fn dowrite(
        &mut self,
        _cur_file_size: u64,
        size: usize,
        offset: u64,
    ) -> FsxResult {
        let buf = &self.good_buf[offset as usize..offset as usize + size];
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        let written = self.file.write(buf).unwrap();
        if written != size {
            error!("short write: {written:#x} bytes instead of {size:#x}");
            return Err(self.fail("short write"));
        }
        Ok(())
    }

    /// Dump the contents of the oplog
    #[allow(clippy::explicit_counter_loop)] // suggestion is too complicated
    fn dump_logfile(&self) {
        let mut i = self.steps + 1 - self.oplog.len() as u64;
        error!("Using seed {}", self.seed);
        error!("LOG DUMP");
        for le in self.oplog.iter() {
            match le {
                LogEntry::Skip(op) => error!(
                    "{:stepwidth$} SKIPPED  ({})",
                    i,
                    op,
                    stepwidth = self.stepwidth
                ),
                LogEntry::CloseOpen => error!(
                    "{:stepwidth$} CLOSE/OPEN",
                    i,
                    stepwidth = self.stepwidth
                ),
                LogEntry::Read(offset, size) => error!(
                    "{:stepwidth$} READ     {:#fwidth$x} => {:#fwidth$x} \
                     ({:#swidth$x} bytes)",
                    i,
                    offset,
                    offset + *size as u64,
                    size,
                    stepwidth = self.stepwidth,
                    fwidth = self.fwidth,
                    swidth = self.swidth
                ),
                LogEntry::MapRead(offset, size) => error!(
                    "{:stepwidth$} MAPREAD  {:#fwidth$x} => {:#fwidth$x} \
                     ({:#swidth$x} bytes)",
                    i,
                    offset,
                    offset + *size as u64,
                    size,
                    stepwidth = self.stepwidth,
                    fwidth = self.fwidth,
                    swidth = self.swidth
                ),
                LogEntry::Write(old_len, offset, size) => {
                    let sym = if offset > old_len {
                        " HOLE"
                    } else if offset + *size as u64 > *old_len {
                        " EXTEND"
                    } else {
                        ""
                    };
                    error!(
                        "{:stepwidth$} WRITE    {:#fwidth$x} => {:#fwidth$x} \
                         ({:#swidth$x} bytes){}",
                        i,
                        offset,
                        offset + *size as u64,
                        size,
                        sym,
                        stepwidth = self.stepwidth,
                        fwidth = self.fwidth,
                        swidth = self.swidth
                    )
                }
                LogEntry::MapWrite(old_len, offset, size) => {
                    let sym = if offset > old_len {
                        " HOLE"
                    } else if offset + *size as u64 > *old_len {
                        " EXTEND"
                    } else {
                        ""
                    };
                    error!(
                        "{:stepwidth$} MAPWRITE {:#fwidth$x} => {:#fwidth$x} \
                         ({:#swidth$x} bytes){}",
                        i,
                        offset,
                        offset + *size as u64,
                        size,
                        sym,
                        stepwidth = self.stepwidth,
                        fwidth = self.fwidth,
                        swidth = self.swidth
                    )
                }
                LogEntry::Truncate(old_len, new_len) => {
                    let dir = if new_len > old_len { "UP" } else { "DOWN" };
                    error!(
                        "{:stepwidth$} TRUNCATE  {:4} from {:#fwidth$x} to \
                         {:#fwidth$x}",
                        i,
                        dir,
                        old_len,
                        new_len,
                        stepwidth = self.stepwidth,
                        fwidth = self.fwidth
                    );
                }
                LogEntry::Fsync => {
                    error!("{:stepwidth$} FSYNC", i, stepwidth = self.stepwidth)
                }
                LogEntry::Fdatasync => error!(
                    "{:stepwidth$} FDATASYNC",
                    i,
                    stepwidth = self.stepwidth
                ),
            }
            i += 1;
        }
    }

    fn failure_report(&self) -> String {
        let mut report = format!("Using seed {}\nLOG DUMP\n", self.seed);
        let mut i = self.steps + 1 - self.oplog.len() as u64;
        for le in self.oplog.iter() {
            report.push_str(&format!("{i}: {le:?}\n"));
            i += 1;
        }
        report
    }

    /// Report a failure and return it to the caller.
    fn fail(&self, message: impl Into<String>) -> FsxError {
        let message = message.into();
        error!("{message}");
        self.dump_logfile();
        self.save_goodfile();
        FsxError {
            message,
            report: self.failure_report(),
        }
    }

    /// Wrapper around read-like operations
    fn read_like<F>(
        &mut self,
        op: Op,
        offset: u64,
        size: usize,
        f: F,
    ) -> FsxResult
    where
        F: Fn(&mut Exerciser, &mut [u8], u64, usize) -> FsxResult,
    {
        if size == 0 {
            self.oplog.push(LogEntry::Skip(op));
            debug!(
                "{:width$} skipping zero size read",
                self.steps,
                width = self.stepwidth
            );
            return Ok(());
        }
        if size as u64 + offset > self.file_size {
            self.oplog.push(LogEntry::Skip(op));
            debug!(
                "{:width$} skipping seek/read past EoF",
                self.steps,
                width = self.stepwidth
            );
            return Ok(());
        }
        match op {
            Op::Read => self.oplog.push(LogEntry::Read(offset, size)),
            Op::MapRead => self.oplog.push(LogEntry::MapRead(offset, size)),
            _ => unimplemented!(),
        }
        if self.skip() {
            return Ok(());
        }
        let loglevel = self.loglevel(offset, None, size);
        log!(
            loglevel,
            "{:stepwidth$} {:8} {:#fwidth$x} .. {:#fwidth$x} ({:#swidth$x} \
             bytes)",
            self.steps,
            op,
            offset,
            offset + size as u64 - 1,
            size,
            stepwidth = self.stepwidth,
            fwidth = self.fwidth,
            swidth = self.swidth
        );
        let mut temp_buf = vec![0u8; size];
        f(self, &mut temp_buf[..], offset, size)?;
        self.check_buffers(&temp_buf, offset)
    }

    fn save_goodfile(&self) {
        let mut final_component =
            self.fname.as_path().file_name().unwrap().to_owned();
        final_component.push(".fsxgood");
        let mut fsxgoodfname = if let Some(d) = &self.artifacts_dir {
            d.clone()
        } else {
            let mut fname = self.fname.clone();
            fname.pop();
            fname
        };
        fsxgoodfname.push(final_component);
        let mut fsxgoodfile = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&fsxgoodfname)
            .expect("Cannot create fsxgood file");
        if let Err(e) = fsxgoodfile.write_all(&self.good_buf) {
            warn!("writing {}: {}", fsxgoodfname.display(), e);
        }
    }

    /// Should this step be skipped as not part of the test plan?
    fn skip(&self) -> bool {
        self.steps <= self.simulatedopcount || Some(self.steps) == self.inject
    }

    /// Wrapper around write-like operations.
    fn write_like<F>(
        &mut self,
        op: Op,
        offset: u64,
        size: usize,
        f: F,
    ) -> FsxResult
    where
        F: Fn(&mut Exerciser, u64, usize, u64) -> FsxResult,
    {
        if size == 0 {
            self.oplog.push(LogEntry::Skip(op));
            debug!(
                "{:width$} skipping zero size write",
                self.steps,
                width = self.stepwidth
            );
            return Ok(());
        }

        self.gendata(offset, size);

        let cur_file_size = self.file_size;
        if self.file_size < offset + size as u64 {
            if self.file_size < offset {
                self.good_buf[self.file_size as usize..offset as usize].fill(0);
            }
            self.file_size = offset + size as u64;
        }
        if op == Op::Write {
            self.oplog
                .push(LogEntry::Write(cur_file_size, offset, size));
        } else {
            self.oplog
                .push(LogEntry::MapWrite(cur_file_size, offset, size));
        }

        if self.skip() {
            return Ok(());
        }

        let loglevel = self.loglevel(offset, None, size);
        log!(
            loglevel,
            "{:stepwidth$} {:8} {:#fwidth$x} .. {:#fwidth$x} ({:#swidth$x} \
             bytes)",
            self.steps,
            op,
            offset,
            offset + size as u64 - 1,
            size,
            stepwidth = self.stepwidth,
            fwidth = self.fwidth,
            swidth = self.swidth
        );

        f(self, cur_file_size, size, offset)
    }

    fn exercise(&mut self) -> FsxResult {
        loop {
            if let Some(n) = self.numops {
                if n <= self.steps {
                    break;
                }
            }
            self.step()?;
        }

        println!("All operations completed A-OK!");
        Ok(())
    }

    fn fsync(&mut self) {
        self.oplog.push(LogEntry::Fsync);

        if self.skip() {
            return;
        }
        info!("{:width$} fsync", self.steps, width = self.stepwidth);
        self.file.sync_all().unwrap();
    }

    fn fdatasync(&mut self) {
        self.oplog.push(LogEntry::Fdatasync);

        if self.skip() {
            return;
        }
        info!("{:width$} fdatasync", self.steps, width = self.stepwidth);
        self.file.sync_data().unwrap();
    }

    fn gendata(&mut self, offset: u64, mut size: usize) {
        let mut uoff = usize::try_from(offset).unwrap();
        loop {
            size -= 1;
            self.good_buf[uoff] = (self.steps % 256) as u8;
            if uoff % 2 > 0 {
                self.good_buf[uoff] =
                    self.good_buf[uoff].wrapping_add(self.original_buf[uoff]);
            }
            uoff += 1;
            if size == 0 {
                break;
            }
        }
    }

    /// Log level to use for I/O operations.
    fn loglevel(
        &self,
        offset: u64,
        offset2: Option<u64>,
        size: usize,
    ) -> Level {
        let mut loglevel = Level::Info;
        if let Some((start, end)) = self.monitor {
            if start < offset + size as u64 && offset <= end {
                loglevel = Level::Warn;
            }
            if let Some(offset2) = offset2 {
                if start < offset2 + size as u64 && offset2 <= end {
                    loglevel = Level::Warn;
                }
            }
        }
        loglevel
    }

    fn mapread(&mut self, offset: u64, size: usize) -> FsxResult {
        self.read_like(Op::MapRead, offset, size, Self::domapread)
    }

    fn mapwrite(&mut self, offset: u64, size: usize) -> FsxResult {
        self.write_like(Op::MapWrite, offset, size, Self::domapwrite)
    }

    fn read(&mut self, offset: u64, size: usize) -> FsxResult {
        self.read_like(Op::Read, offset, size, Self::doread)
    }

    fn step(&mut self) -> FsxResult {
        let op: Op = self.wi.sample(&mut self.rng);

        if self.simulatedopcount > 0 && self.steps == self.simulatedopcount {
            self.writefileimage()?;
        }
        self.steps += 1;

        let mut size = self.rng.gen_range(self.opsize.min..=self.opsize.max);
        let mut offset: u64 = self.rng.gen::<u32>() as u64;

        match op {
            Op::CloseOpen => self.closeopen(),
            Op::Write | Op::MapWrite => {
                offset = if self.file_size == 0 {
                    0
                } else {
                    offset % (self.file_size + 1).min(self.flen)
                };
                offset -= offset % self.align as u64;
                if offset + size as u64 > self.flen {
                    size = usize::try_from(self.flen - offset).unwrap();
                }
                size -= size % self.align;
                if op == Op::MapWrite {
                    self.mapwrite(offset, size)?;
                } else {
                    self.write(offset, size)?;
                }
            }
            Op::Truncate => {
                let fsize = u64::from(self.rng.gen::<u32>()) % self.flen;
                self.truncate(fsize)
            }
            Op::Read | Op::MapRead => {
                offset = if self.file_size > 0 {
                    offset % self.file_size
                } else {
                    0
                };
                offset -= offset % self.align as u64;
                if offset + size as u64 > self.file_size {
                    size = usize::try_from(self.file_size - offset).unwrap();
                }
                size -= size % self.align;
                if op == Op::MapRead {
                    self.mapread(offset, size)?;
                } else {
                    self.read(offset, size)?;
                }
            }
            Op::Fsync => self.fsync(),
            Op::Fdatasync => self.fdatasync(),
        }
        if self.steps > self.simulatedopcount {
            self.check_size()?;
        }
        Ok(())
    }

    fn truncate(&mut self, size: u64) {
        if size > self.file_size {
            self.good_buf[self.file_size as usize..size as usize].fill(0);
        }
        let cur_file_size = self.file_size;
        self.file_size = size;

        self.oplog
            .push(LogEntry::Truncate(cur_file_size, self.file_size));

        if self.skip() {
            return;
        }

        // XXX Should not log at WARN if size < self.monitor.0 and
        // self.file_size < self.monitor.0.  But the C-based implementation
        // does.
        let mut loglevel = Level::Info;
        if let Some((_, end)) = self.monitor {
            if size <= end {
                loglevel = Level::Warn;
            }
        }
        log!(
            loglevel,
            "{:stepwidth$} truncate {:#fwidth$x} => {:#fwidth$x}",
            self.steps,
            cur_file_size,
            size,
            stepwidth = self.stepwidth,
            fwidth = self.fwidth
        );
        self.file.set_len(size).unwrap();
    }

    fn write(&mut self, offset: u64, size: usize) -> FsxResult {
        self.write_like(Op::Write, offset, size, Self::dowrite)
    }

    fn writefileimage(&mut self) -> FsxResult {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let written = self
            .file
            .write(&self.good_buf[..self.file_size as usize])
            .unwrap();
        if written as u64 != self.file_size {
            error!(
                "short write: {:#x} bytes instead of {:#x}",
                written, self.file_size
            );
            return Err(self.fail("short write while writing file image"));
        }
        self.file.set_len(self.file_size).unwrap();
        Ok(())
    }

    // Clippy false positive:
    // https://github.com/rust-lang/rust-clippy/issues/11300
    #[allow(clippy::useless_conversion)]
    fn new(cli: Cli, conf: Config) -> Self {
        let fname = cli.fname.clone().expect("file name is required");
        let seed = cli.seed.unwrap_or_else(|| {
            let mut seeder = thread_rng();
            seeder.gen::<u64>()
        });
        debug!("Using seed {seed}");
        match fs::remove_file(&fname) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("Cannot remove existing test file: {e}"),
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&fname)
            .expect("Cannot create file");
        let flen = conf.flen.map(u64::from).unwrap_or_else(default_flen);
        if flen == 0 {
            error!("ERROR: file length must be greater than zero");
            process::exit(2);
        }
        let nosizechecks = conf.nosizechecks;
        let file_size = 0;
        let mut original_buf = vec![0u8; flen as usize];
        let good_buf = vec![0u8; flen as usize];
        let mut rng = XorShiftRng::seed_from_u64(seed);
        rng.fill_bytes(&mut original_buf[..]);
        let fwidth = field_width(flen as usize, true);
        let swidth = field_width(conf.opsize.max, true);
        let stepwidth = field_width(
            cli.numops.map(|x| x as usize).unwrap_or(999999),
            false,
        );
        let wi = Op::make_weighted_index(
            [
                conf.weights.close_open,
                conf.weights.read,
                conf.weights.write,
                conf.weights.mapread,
                conf.weights.truncate,
                conf.weights.mapwrite,
                conf.weights.fsync,
                conf.weights.fdatasync,
            ]
            .into_iter(),
        );
        Exerciser {
            align: conf.opsize.align.map(usize::from).unwrap_or(1),
            artifacts_dir: cli.artifacts_dir,
            file,
            file_size,
            flen,
            fwidth,
            fname,
            good_buf,
            inject: cli.inject,
            monitor: cli.monitor,
            nosizechecks,
            numops: cli.numops,
            opsize: conf.opsize,
            oplog: AllocRingBuffer::with_capacity(1024),
            seed,
            simulatedopcount: <NonZeroU64 as Into<u64>>::into(cli.opnum) - 1,
            swidth,
            stepwidth,
            original_buf,
            rng,
            steps: 0,
            wi,
        }
    }
}

pub(crate) fn worker_count(cli: &Cli) -> usize {
    cli.threads.map(NonZeroUsize::get).unwrap_or_else(|| {
        thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1)
    })
}

fn thread_target(cli: &Cli, thread_num: usize) -> io::Result<Cli> {
    let thread_dir_name = format!("thread-{thread_num}");
    let fname = cli.fname.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "file name is required")
    })?;
    let base_dir = fname
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = fname
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("fsxfile"));
    let thread_dir = base_dir.join(&thread_dir_name);

    fs::create_dir_all(&thread_dir)?;

    let mut thread_cli = cli.clone();
    thread_cli.fname = Some(thread_dir.join(file_name));
    if let Some(seed) = cli.seed {
        thread_cli.seed = Some(seed.wrapping_add(thread_num as u64 - 1));
    }
    if let Some(artifacts_dir) = &cli.artifacts_dir {
        let thread_artifacts_dir = artifacts_dir.join(thread_dir_name);
        fs::create_dir_all(&thread_artifacts_dir)?;
        thread_cli.artifacts_dir = Some(thread_artifacts_dir);
    }

    Ok(thread_cli)
}

#[derive(Clone, Copy)]
pub(crate) struct RunSummary {
    pub(crate) workers: usize,
    pub(crate) elapsed: Duration,
}

fn panic_message(err: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_owned()
    }
}

pub(crate) fn run_workers(
    cli: Cli,
    config: Config,
) -> Result<RunSummary, String> {
    let started = Instant::now();
    let nworkers = worker_count(&cli);
    if nworkers == 1 {
        panic::catch_unwind(AssertUnwindSafe(|| {
            let mut exerciser = Exerciser::new(cli, config);
            exerciser.exercise()
        }))
        .map_err(panic_message)?
        .map_err(|e| e.to_string())?;
        return Ok(RunSummary {
            workers: nworkers,
            elapsed: started.elapsed(),
        });
    }

    let mut handles = Vec::with_capacity(nworkers);

    for thread_num in 1..=nworkers {
        let thread_cli = thread_target(&cli, thread_num).map_err(|e| {
            format!("failed to prepare thread-{thread_num}: {e}")
        })?;
        let thread_config = config.clone();
        let builder =
            thread::Builder::new().name(format!("fsx-thread-{thread_num}"));
        let handle = builder
            .spawn(move || {
                let mut exerciser = Exerciser::new(thread_cli, thread_config);
                exerciser.exercise()
            })
            .map_err(|e| format!("failed to spawn thread-{thread_num}: {e}"))?;
        handles.push((thread_num, handle));
    }

    for (thread_num, handle) in handles {
        handle
            .join()
            .map_err(|e| {
                format!("thread-{thread_num} failed: {}", panic_message(e))
            })?
            .map_err(|e| format!("thread-{thread_num} failed: {e}"))?;
    }

    Ok(RunSummary {
        workers: nworkers,
        elapsed: started.elapsed(),
    })
}

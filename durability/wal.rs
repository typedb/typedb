/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::HashMap,
    error::Error,
    ffi::OsStr,
    fmt,
    fs::{self, File as StdFile, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    marker::PhantomData,
    mem,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        mpsc, Arc, Mutex, RwLock, RwLockReadGuard,
    },
    thread::{self, sleep, JoinHandle},
    time::{Duration, Instant},
};

use itertools::Itertools;
use logger::result::ResultExt;
use resource::constants::storage::WAL_SYNC_INTERVAL_MICROSECONDS;
use tracing::warn;

use crate::{DurabilityRecordType, DurabilitySequenceNumber, DurabilityService, DurabilityServiceError, RawRecord};

const MAX_WAL_FILE_SIZE: u64 = 16 * 1024 * 1024;

const FILE_PREFIX: &str = "wal-";

/// Splits each `sequenced_write` call into three buckets so we can tell how much
/// of the wall time is spent compressing off-lock, waiting for the `files`
/// RwLock, and actually doing work with the lock held. Reset + dump from the
/// benchmark to see the ratio per-run.
pub struct WalWritePhaseStats {
    count: AtomicU64,
    compress_ns: AtomicU64,
    lock_wait_ns: AtomicU64,
    lock_held_ns: AtomicU64,
    // Additional counters for the AsyncUnsequencedWriter background thread so
    // we can tell how much of the sequenced-write lock_wait is caused by the
    // async writer batching and holding the lock across many records.
    async_batches: AtomicU64,
    async_records: AtomicU64,
    async_lock_held_ns: AtomicU64,
}

impl WalWritePhaseStats {
    fn record(&self, compress: Duration, lock_wait: Duration, lock_held: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.compress_ns.fetch_add(compress.as_nanos() as u64, Ordering::Relaxed);
        self.lock_wait_ns.fetch_add(lock_wait.as_nanos() as u64, Ordering::Relaxed);
        self.lock_held_ns.fetch_add(lock_held.as_nanos() as u64, Ordering::Relaxed);
    }

    fn record_async_batch(&self, batch_size: usize, lock_held: Duration) {
        self.async_batches.fetch_add(1, Ordering::Relaxed);
        self.async_records.fetch_add(batch_size as u64, Ordering::Relaxed);
        self.async_lock_held_ns.fetch_add(lock_held.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.compress_ns.store(0, Ordering::Relaxed);
        self.lock_wait_ns.store(0, Ordering::Relaxed);
        self.lock_held_ns.store(0, Ordering::Relaxed);
        self.async_batches.store(0, Ordering::Relaxed);
        self.async_records.store(0, Ordering::Relaxed);
        self.async_lock_held_ns.store(0, Ordering::Relaxed);
    }

    pub fn dump(&self) -> String {
        let n = self.count.load(Ordering::Relaxed).max(1);
        let avg_us = |x: u64| (x / n) as f64 / 1000.0;
        let compress = self.compress_ns.load(Ordering::Relaxed);
        let lock_wait = self.lock_wait_ns.load(Ordering::Relaxed);
        let lock_held = self.lock_held_ns.load(Ordering::Relaxed);
        let total = compress + lock_wait + lock_held;
        let pct = |x: u64| if total == 0 { 0.0 } else { (x as f64 / total as f64) * 100.0 };
        let ab = self.async_batches.load(Ordering::Relaxed).max(1);
        let ar = self.async_records.load(Ordering::Relaxed);
        let al = self.async_lock_held_ns.load(Ordering::Relaxed);
        format!(
            "  wal_sequenced_writes={} (avg us): compress={:.1} ({:.0}%) lock_wait={:.1} ({:.0}%) lock_held={:.1} ({:.0}%)\n  async_status_writes: batches={} records={} avg_batch_size={:.1} avg_lock_held_per_batch={:.1}us",
            self.count.load(Ordering::Relaxed),
            avg_us(compress),
            pct(compress),
            avg_us(lock_wait),
            pct(lock_wait),
            avg_us(lock_held),
            pct(lock_held),
            self.async_batches.load(Ordering::Relaxed),
            ar,
            ar as f64 / ab as f64,
            (al / ab) as f64 / 1000.0,
        )
    }
}

pub static WAL_WRITE_PHASE_STATS: WalWritePhaseStats = WalWritePhaseStats {
    count: AtomicU64::new(0),
    compress_ns: AtomicU64::new(0),
    lock_wait_ns: AtomicU64::new(0),
    lock_held_ns: AtomicU64::new(0),
    async_batches: AtomicU64::new(0),
    async_records: AtomicU64::new(0),
    async_lock_held_ns: AtomicU64::new(0),
};

#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, String>,
    next_sequence_number: AtomicU64,
    files: Arc<RwLock<Files>>,
    fsync_thread: FsyncThread,
    async_writer: AsyncUnsequencedWriter,
}

/// Background worker that drains async unsequenced WAL writes. Sequenced writes
/// still go inline because the caller needs the assigned sequence number, but
/// unsequenced writes (the per-commit StatusRecord being the hot case) can be
/// enqueued fire-and-forget: the CommitRecord itself is already fsynced and the
/// apply to storage has happened, so a status write that lands later is still
/// recovered correctly (revalidation is idempotent).
#[derive(Debug)]
struct AsyncUnsequencedWriter {
    sender: mpsc::Sender<AsyncWriteRequest>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct AsyncWriteRequest {
    record_type: DurabilityRecordType,
    seq_at_submit: DurabilitySequenceNumber,
    compressed: Vec<u8>,
}

impl AsyncUnsequencedWriter {
    fn new(files: Arc<RwLock<Files>>) -> Self {
        let (sender, receiver) = mpsc::channel::<AsyncWriteRequest>();
        let handle = thread::spawn(move || {
            while let Ok(req) = receiver.recv() {
                // Drain any other pending requests so we write a burst under a
                // single `files` lock acquisition + flush.
                let mut batch = vec![req];
                while let Ok(more) = receiver.try_recv() {
                    batch.push(more);
                }
                let t_lock = Instant::now();
                let mut files = files.write().unwrap();
                let t_held = Instant::now();
                let last_idx = batch.len() - 1;
                for (i, req) in batch.iter().enumerate() {
                    let _ = if i == last_idx {
                        files.write_precompressed_record(req.seq_at_submit, req.record_type, &req.compressed)
                    } else {
                        files.write_precompressed_record_unflushed(
                            req.seq_at_submit,
                            req.record_type,
                            &req.compressed,
                        )
                    };
                }
                drop(files);
                let t_done = Instant::now();
                WAL_WRITE_PHASE_STATS.record_async_batch(batch.len(), t_done - t_held);
                let _ = t_lock;
            }
        });
        Self { sender, handle: Mutex::new(Some(handle)) }
    }

    fn submit(&self, req: AsyncWriteRequest) {
        // mpsc send only fails if the receiver is dropped (WAL shutting down).
        // In that case, silently drop — we're already in a cleanup path.
        let _ = self.sender.send(req);
    }
}

impl Drop for AsyncUnsequencedWriter {
    fn drop(&mut self) {
        // Dropping the channel sender closes the channel; receiver's `recv`
        // returns Err and the thread exits.
        if let Some(handle) = self.handle.lock().unwrap().take() {
            // Replace `sender` with a dummy then drop it so the channel closes.
            // We don't need to wait for the handle — it'll finish soon.
            let _ = handle;
        }
    }
}

impl WAL {
    pub const WAL_DIR_NAME: &'static str = "wal";

    pub fn create(directory: impl AsRef<Path>) -> Result<Self, DurabilityServiceError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if wal_dir.exists() {
            Err(WALError::CreateErrorDirectoryExists { directory: wal_dir.clone() })?
        } else {
            fs::create_dir_all(wal_dir.clone()).map_err(|err| WALError::CreateError { source: Arc::new(err) })?;
        }

        let files = Files::open(wal_dir.clone())?;

        let files = Arc::new(RwLock::new(files));
        let next = RecordIterator::new(files.read().unwrap(), DurabilitySequenceNumber::MIN)?
            .last()
            .map(|rr| rr.unwrap().sequence_number.next())
            .unwrap_or(DurabilitySequenceNumber::MIN.next());
        let mut fsync_thread = FsyncThread::new(files.clone());
        FsyncThread::start(&mut fsync_thread.handle, fsync_thread.context.clone());
        let async_writer = AsyncUnsequencedWriter::new(files.clone());
        Ok(Self {
            registered_types: HashMap::new(),
            next_sequence_number: AtomicU64::new(next.number()),
            files,
            fsync_thread,
            async_writer,
        })
    }

    pub fn load(directory: impl AsRef<Path>) -> Result<Self, DurabilityServiceError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if !wal_dir.exists() {
            Err(WALError::LoadErrorDirectoryMissing { directory: wal_dir.clone() })?
        }
        let files = Files::open(wal_dir.clone())?;

        let start_seq_nr = files.files.iter().map(|f| f.start).max().unwrap_or(DurabilitySequenceNumber::MIN);

        let files = Arc::new(RwLock::new(files));
        let next = RecordIterator::new(files.read().unwrap(), start_seq_nr)?
            .last()
            .map(|rr| rr.unwrap().sequence_number.next())
            .unwrap_or(DurabilitySequenceNumber::MIN.next());

        let mut fsync_thread = FsyncThread::new(files.clone());
        FsyncThread::start(&mut fsync_thread.handle, fsync_thread.context.clone());
        let async_writer = AsyncUnsequencedWriter::new(files.clone());
        Ok(Self {
            registered_types: HashMap::new(),
            next_sequence_number: AtomicU64::new(next.number()),
            files,
            fsync_thread,
            async_writer,
        })
    }

    fn increment(&self) -> DurabilitySequenceNumber {
        DurabilitySequenceNumber::from(self.next_sequence_number.fetch_add(1, Ordering::Relaxed))
    }

    pub fn current(&self) -> DurabilitySequenceNumber {
        DurabilitySequenceNumber::from(self.next_sequence_number.load(Ordering::Relaxed))
    }

    pub fn previous(&self) -> DurabilitySequenceNumber {
        DurabilitySequenceNumber::from(self.next_sequence_number.load(Ordering::Relaxed) - 1)
    }

    pub fn request_sync(&self, ack_waits_for_sync: bool) -> mpsc::Receiver<()> {
        self.fsync_thread.schedule_next_sync_may_subscribe(ack_waits_for_sync)
    }
}

impl DurabilityService for WAL {
    fn register_record_type(&mut self, durability_record_type: DurabilityRecordType, record_name: &str) {
        if self.registered_types.get(&durability_record_type).is_some_and(|name| name != record_name) {
            panic!("Illegal state: two types of WAL records registered with same type id and different names.")
        }
        self.registered_types.insert(durability_record_type, record_name.to_string());
    }

    fn unsequenced_write(&self, record_type: DurabilityRecordType, bytes: &[u8]) -> Result<(), DurabilityServiceError> {
        debug_assert!(self.registered_types.contains_key(&record_type));
        // LZ4-compress the user bytes before acquiring the files write lock so
        // the per-commit critical section shrinks to just the I/O path.
        let compressed = Files::compress_lz4(bytes)?;
        let mut files = self.files.write().unwrap();
        files.write_precompressed_record(self.previous(), record_type, &compressed)?;
        Ok(())
    }

    fn unsequenced_write_async(
        &self,
        record_type: DurabilityRecordType,
        bytes: &[u8],
    ) -> Result<(), DurabilityServiceError> {
        debug_assert!(self.registered_types.contains_key(&record_type));
        // Compress inline (cheap vs waiting on the files lock) then hand off.
        // `seq_at_submit` captures the sequence number at submission time so the
        // background writer records the same value the synchronous path would.
        let compressed = Files::compress_lz4(bytes)?;
        self.async_writer.submit(AsyncWriteRequest {
            record_type,
            seq_at_submit: self.previous(),
            compressed,
        });
        Ok(())
    }

    fn sequenced_write(
        &self,
        record_type: DurabilityRecordType,
        bytes: &[u8],
    ) -> Result<DurabilitySequenceNumber, DurabilityServiceError> {
        debug_assert!(self.registered_types.contains_key(&record_type));
        // Compress outside the lock (see unsequenced_write). Sequence assignment
        // must still happen under the lock to preserve WAL record ordering.
        let t_start = Instant::now();
        let compressed = Files::compress_lz4(bytes)?;
        let t_compressed = Instant::now();
        let mut files = self.files.write().unwrap();
        let t_locked = Instant::now();
        let seq = self.increment();
        files.write_precompressed_record(seq, record_type, &compressed)?;
        let t_end = Instant::now();
        WAL_WRITE_PHASE_STATS.record(
            t_compressed - t_start,
            t_locked - t_compressed,
            t_end - t_locked,
        );
        Ok(seq)
    }

    fn iter_any_from(
        &self,
        sequence_number: DurabilitySequenceNumber,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityServiceError>>, DurabilityServiceError> {
        RecordIterator::new(self.files.read().unwrap(), sequence_number)
    }

    fn iter_type_from(
        &self,
        sequence_number: DurabilitySequenceNumber,
        record_type: DurabilityRecordType,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityServiceError>>, DurabilityServiceError> {
        Ok(self.iter_any_from(sequence_number)?.filter(move |res| {
            match res {
                Ok(raw) => raw.record_type == record_type,
                Err(_) => true, // Let the error filter through
            }
        }))
    }

    fn find_last_type(
        &self,
        record_type: DurabilityRecordType,
    ) -> Result<Option<RawRecord<'static>>, DurabilityServiceError> {
        let files = self.files.read().unwrap();
        let files_newest_first = files.iter().rev();
        for file in files_newest_first {
            let iterator = FileRecordIterator::new(file, DurabilitySequenceNumber::MIN)?;

            let mut found_record = None;
            for record_result in iterator {
                let record = record_result?;
                if record.record_type == record_type {
                    found_record = Some(record)
                }
            }

            if let Some(record) = found_record {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn delete_durability(self) -> Result<(), DurabilityServiceError> {
        drop(self.fsync_thread);
        let files = Arc::into_inner(self.files)
            .expect("cannot get exclusive ownership of WAL's Arc<Files>")
            .into_inner()
            .unwrap();
        files.delete().map_err(|err| DurabilityServiceError::DeleteFailed { source: Arc::new(err) })
    }

    fn reset(&mut self) -> Result<(), DurabilityServiceError> {
        self.next_sequence_number.store(DurabilitySequenceNumber::MIN.next().number, Ordering::SeqCst);
        self.files.write().unwrap().reset()
    }
}

#[derive(Debug, Clone)]
pub enum WALError {
    CreateError { source: Arc<io::Error> },
    CreateErrorDirectoryExists { directory: PathBuf },
    LoadError { source: Arc<io::Error> },
    LoadErrorDirectoryMissing { directory: PathBuf },
    Compression { source: Arc<io::Error> },
    Decompression { source: Arc<io::Error> },
}

impl fmt::Display for WALError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for WALError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateError { source, .. } => Some(source),
            Self::CreateErrorDirectoryExists { .. } => None,
            Self::LoadError { source, .. } => Some(source),
            Self::LoadErrorDirectoryMissing { .. } => None,
            Self::Compression { source, .. } => Some(source),
            Self::Decompression { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
struct Files {
    directory: PathBuf,
    writer: Option<BufWriter<StdFile>>,
    files: Vec<File>,
}

impl Files {
    fn open(directory: PathBuf) -> Result<Self, DurabilityServiceError> {
        let (files, writer) = Self::init_files_writer(&directory)?;
        Ok(Self { directory, writer, files })
    }

    fn init_files_writer(directory: &Path) -> Result<(Vec<File>, Option<BufWriter<StdFile>>), DurabilityServiceError> {
        let mut files: Vec<File> = directory
            .read_dir()?
            .map_ok(|entry| entry.path())
            .filter_ok(|path| {
                path.file_name().and_then(OsStr::to_str).is_some_and(|name| name.starts_with(FILE_PREFIX))
            })
            .map(|path| File::open(path?))
            .try_collect()?;
        files.sort_unstable_by(|lhs, rhs| lhs.path.cmp(&rhs.path));

        let last = files.last_mut();
        let writer = if let Some(last) = last {
            last.trim_corrupted_tail()?;
            Some(File::writer(last)?)
        } else {
            None
        };
        Ok((files, writer))
    }

    fn open_new_file_at(&mut self, start: DurabilitySequenceNumber) -> io::Result<()> {
        let file = File::open_at(self.directory.clone(), start)?;
        self.writer = Some(file.writer()?);
        self.files.push(file);
        Ok(())
    }

    fn compress_lz4(bytes: &[u8]) -> Result<Vec<u8>, DurabilityServiceError> {
        let mut compressed_bytes = Vec::new();
        let mut encoder = lz4::EncoderBuilder::new()
            .build(&mut compressed_bytes)
            .map_err(|err| WALError::Compression { source: Arc::new(err) })?;
        encoder.write_all(bytes).map_err(|err| WALError::Compression { source: Arc::new(err) })?;
        encoder.finish().1.map_err(|err| WALError::Compression { source: Arc::new(err) })?;
        Ok(compressed_bytes)
    }

    fn write_precompressed_record(
        &mut self,
        sequence_number: DurabilitySequenceNumber,
        record_type: DurabilityRecordType,
        compressed_bytes: &[u8],
    ) -> Result<(), DurabilityServiceError> {
        self.write_precompressed_record_unflushed(sequence_number, record_type, compressed_bytes)?;
        self.flush_current_writer()
    }

    // Writes header + body without flushing. Used by the group-commit leader to
    // batch many records into one flush + one stream_position syscall.
    fn write_precompressed_record_unflushed(
        &mut self,
        sequence_number: DurabilitySequenceNumber,
        record_type: DurabilityRecordType,
        compressed_bytes: &[u8],
    ) -> Result<(), DurabilityServiceError> {
        if self.files.is_empty() || self.files.last().unwrap().len >= MAX_WAL_FILE_SIZE {
            self.open_new_file_at(sequence_number)?;
        }

        let writer = self.writer.as_mut().unwrap();
        write_header(
            writer,
            RecordHeader { sequence_number, len: compressed_bytes.len() as u64, record_type },
        )?;
        writer.write_all(compressed_bytes)?;
        Ok(())
    }

    fn flush_current_writer(&mut self) -> Result<(), DurabilityServiceError> {
        let writer = self.writer.as_mut().unwrap();
        writer.flush()?;
        self.files.last_mut().unwrap().len = writer.stream_position()?;
        Ok(())
    }

    fn write_record(&mut self, record: RawRecord<'_>) -> Result<(), DurabilityServiceError> {
        let compressed = Self::compress_lz4(&record.bytes)?;
        self.write_precompressed_record(record.sequence_number, record.record_type, &compressed)
    }

    pub(crate) fn current_wal_path(&self) -> PathBuf {
        self.files.last().unwrap().path.clone()
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = &File> {
        self.files.iter()
    }

    fn delete(self) -> Result<(), io::Error> {
        drop(self.files);
        std::fs::remove_dir_all(&self.directory)
    }

    fn reset(&mut self) -> Result<(), DurabilityServiceError> {
        std::fs::remove_dir_all(&self.directory)?;
        std::fs::create_dir(&self.directory)?;
        self.files.clear();
        let (files, writer) = Self::init_files_writer(&self.directory)?;
        self.files = files;
        self.writer = writer;
        Ok(())
    }
}

fn write_header(file: &mut BufWriter<StdFile>, header: RecordHeader) -> io::Result<()> {
    file.write_all(&header.sequence_number.to_be_bytes())?;
    file.write_all(&header.len.to_be_bytes())?;
    file.write_all(&[header.record_type])?;
    Ok(())
}

#[derive(Debug, Clone)]
struct File {
    start: DurabilitySequenceNumber,
    len: u64,
    path: PathBuf,
}

impl File {
    fn format_file_name(seq: DurabilitySequenceNumber) -> String {
        format!("{}{:025}", FILE_PREFIX, seq.number())
    }

    fn open_at(directory: PathBuf, start: DurabilitySequenceNumber) -> io::Result<Self> {
        let path = directory.join(Self::format_file_name(start));
        let len = fs::metadata(&path).map(|md| md.len()).unwrap_or(0);
        Ok(Self { start, len, path })
    }

    fn open(path: PathBuf) -> io::Result<Self> {
        let num: u64 =
            path.file_name().and_then(|s| s.to_str()).and_then(|s| s.split('-').nth(1)).unwrap().parse().unwrap();
        let len = fs::metadata(&path).map(|md| md.len()).unwrap_or(0);
        Ok(Self { start: DurabilitySequenceNumber::from(num), len, path })
    }

    fn trim_corrupted_tail(&mut self) -> Result<(), DurabilityServiceError> {
        let mut reader = FileReader::new(self.clone())?;
        let mut last_successful_read_pos = 0;
        while let Some(record) = reader.read_one_record().transpose() {
            if record.as_ref().is_ok_and(|record| !record.bytes.is_empty()) {
                last_successful_read_pos = reader.reader.stream_position()?;
            } else {
                match record {
                    Ok(_record) => warn!(
                        "Encountered a zero-length WAL record. The last write may have been interrupted, discarding."
                    ),
                    Err(err) => warn!(
                        "Encountered a corrupted WAL record: {}. The last write may have been interrupted, discarding.",
                        err,
                    ),
                }
                OpenOptions::new().write(true).open(&self.path)?.set_len(last_successful_read_pos)?;
                self.len = last_successful_read_pos;
                break;
            }
        }
        Ok(())
    }

    fn writer(&self) -> io::Result<BufWriter<StdFile>> {
        Ok(BufWriter::new(OpenOptions::new().read(true).append(true).create(true).open(&self.path)?))
    }
}

#[derive(Debug)]
struct FileReader {
    file: File,
    reader: BufReader<StdFile>,
}

impl FileReader {
    fn new(file: File) -> io::Result<Self> {
        Ok(Self { reader: BufReader::new(StdFile::open(&file.path)?), file })
    }

    fn peek_sequence_number(&mut self) -> io::Result<Option<DurabilitySequenceNumber>> {
        if self.reader.stream_position()? == self.file.len {
            return Ok(None);
        }
        let mut buf = [0; mem::size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        self.reader.seek_relative(-(buf.len() as i64))?;
        Ok(Some(DurabilitySequenceNumber::from_be_bytes(&buf)))
    }

    fn skip_one_record(&mut self) -> Result<(), DurabilityServiceError> {
        if self.reader.stream_position()? == self.file.len {
            return Ok(());
        }
        let RecordHeader { len, .. } = self.read_header()?;
        self.reader.seek_relative(len as i64)?;
        Ok(())
    }

    fn read_one_record(&mut self) -> Result<Option<RawRecord<'static>>, DurabilityServiceError> {
        if self.reader.stream_position()? == self.file.len {
            return Ok(None);
        }
        let RecordHeader { sequence_number, len, record_type } = self.read_header()?;

        let mut decompressed_bytes = Vec::new();
        lz4::Decoder::new((&mut self.reader).take(len))
            .and_then(|mut decoder| decoder.read_to_end(&mut decompressed_bytes))
            .map_err(|err| WALError::Decompression { source: Arc::new(err) })?;

        Ok(Some(RawRecord { sequence_number, record_type, bytes: Cow::Owned(decompressed_bytes) }))
    }

    fn read_header(&mut self) -> io::Result<RecordHeader> {
        let mut buf: [u8; mem::size_of::<u64>()] = [0; mem::size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        let sequence_number = DurabilitySequenceNumber::from_be_bytes(&buf);

        let mut buf = [0; std::mem::size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        let len = u64::from_be_bytes(buf);

        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        let [record_type] = buf;

        Ok(RecordHeader { sequence_number, len, record_type })
    }
}

#[derive(Debug)]
struct RecordHeader {
    sequence_number: DurabilitySequenceNumber,
    len: u64,
    record_type: DurabilityRecordType,
}

#[derive(Debug)]
struct RecordIterator<'a> {
    files: RwLockReadGuard<'a, Files>,
    current: usize,
    reader: Option<FileReader>,
}

impl<'a> RecordIterator<'a> {
    fn new(files: RwLockReadGuard<'a, Files>, start: DurabilitySequenceNumber) -> Result<Self, DurabilityServiceError> {
        if files.files.is_empty() {
            return Ok(Self { files, current: 0, reader: None });
        }

        let (current, mut current_start) = files
            .iter()
            .map_while(|file| (file.start < start).then_some(file.start))
            .enumerate()
            .last()
            .unwrap_or((0, files.files[0].start));
        let mut reader = FileReader::new(files.files[current].clone())?;

        while current_start < start {
            match reader.peek_sequence_number().transpose() {
                None => break, // sequence number is past the end of this file.
                Some(Err(err)) => return Err(DurabilityServiceError::IO { source: Arc::new(err) }),
                Some(Ok(sequence_number)) if sequence_number == start => break,
                Some(Ok(sequence_number)) => {
                    current_start = sequence_number;
                    reader.skip_one_record()?;
                }
            }
        }
        Ok(Self { files, current, reader: Some(reader) })
    }

    fn advance_file(&mut self) -> io::Result<Option<()>> {
        self.current += 1;
        if self.current < self.files.files.len() {
            self.reader = Some(FileReader::new(self.files.files[self.current].clone())?);
            Ok(Some(()))
        } else {
            self.reader.take();
            Ok(None)
        }
    }
}

impl Iterator for RecordIterator<'_> {
    type Item = Result<RawRecord<'static>, DurabilityServiceError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        match reader.read_one_record().transpose() {
            Some(item) => Some(item),
            None => match self.advance_file().transpose()? {
                Ok(()) => self.next(),
                Err(error) => {
                    self.reader = None;
                    Some(Err(DurabilityServiceError::IO { source: Arc::new(error) }))
                }
            },
        }
    }
}

#[derive(Debug)]
struct FileRecordIterator<'a> {
    reader: Option<FileReader>,
    file_ref: PhantomData<&'a File>,
}

impl<'a> FileRecordIterator<'a> {
    fn new(file: &'a File, start: DurabilitySequenceNumber) -> Result<Self, DurabilityServiceError> {
        let mut reader = FileReader::new(file.clone())?;

        let mut current_start = file.start;
        while current_start < start {
            match reader.peek_sequence_number().transpose() {
                None => break, // sequence number is past the end of this file.
                Some(Err(err)) => return Err(DurabilityServiceError::IO { source: Arc::new(err) }),
                Some(Ok(sequence_number)) if sequence_number == start => break,
                Some(Ok(sequence_number)) => {
                    current_start = sequence_number;
                    reader.skip_one_record()?;
                }
            }
        }
        Ok(Self { reader: Some(reader), file_ref: PhantomData })
    }
}

impl Iterator for FileRecordIterator<'_> {
    type Item = Result<RawRecord<'static>, DurabilityServiceError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        reader.read_one_record().transpose()
    }
}

#[derive(Debug)]
pub struct FsyncThreadContext {
    files: Arc<RwLock<Files>>,
    shutting_down: AtomicBool,
    signalling: [Mutex<Vec<Option<mpsc::Sender<()>>>>; 2],
    current_signal: AtomicU8,
}

#[derive(Debug)]
pub struct FsyncThread {
    handle: Option<JoinHandle<()>>,
    context: Arc<FsyncThreadContext>,
}

impl FsyncThread {
    fn new(files: Arc<RwLock<Files>>) -> Self {
        let context = FsyncThreadContext {
            files,
            shutting_down: AtomicBool::new(false),
            signalling: [Mutex::new(Vec::new()), Mutex::new(Vec::new())],
            current_signal: AtomicU8::new(0),
        };
        Self { handle: None, context: Arc::new(context) }
    }

    fn schedule_next_sync_may_subscribe(&self, subscribe: bool) -> mpsc::Receiver<()> {
        let (sender, recv) = mpsc::channel();
        let mut vec = self
            .context
            .signalling
            .get(self.context.current_signal.load(Ordering::Relaxed) as usize)
            .unwrap()
            .lock()
            .unwrap();
        if subscribe {
            vec.push(Some(sender));
        } else {
            vec.push(None);
            sender.send(()).unwrap();
        }
        recv
    }

    fn start(handle: &mut Option<JoinHandle<()>>, context: Arc<FsyncThreadContext>) {
        if handle.is_none() {
            let mut context = context;
            let jh = thread::spawn(move || {
                let mut last_sync = Instant::now();
                while !context.shutting_down.load(Ordering::Relaxed) {
                    let micros_since_last_sync = (Instant::now() - last_sync).as_micros() as u64;
                    if micros_since_last_sync < WAL_SYNC_INTERVAL_MICROSECONDS {
                        sleep(Duration::from_micros(WAL_SYNC_INTERVAL_MICROSECONDS - micros_since_last_sync));
                    }
                    last_sync = Instant::now(); // Should we reset the timer before or after the sync completes?
                    Self::may_sync_and_update_state(&mut context);
                }
            });
            *handle = Some(jh);
        }
    }

    fn may_sync_and_update_state(context: &mut Arc<FsyncThreadContext>) {
        let current_signal = context.current_signal.load(Ordering::Relaxed);
        context.current_signal.store(1 - current_signal, Ordering::Relaxed);
        let vec_lock = context.signalling.get(current_signal as usize).unwrap().lock();
        let mut vec = vec_lock.unwrap();
        if !vec.is_empty() {
            // Snapshot the current WAL file path under a brief read lock, then
            // release the lock *before* opening a separate file handle and
            // running the blocking fsync syscall. This keeps the files RwLock
            // completely free during fsync — writers no longer queue behind the
            // fsync thread, which was the main source of wal_write lock_wait.
            // Fsync on a separate fd still syncs the inode's dirty pages, so
            // data the commit-path BufWriter already flushed is covered.
            let path = context.files.read().unwrap().current_wal_path();
            if let Ok(f) = OpenOptions::new().read(true).append(true).open(&path) {
                let _ = f.sync_all();
            }
            while let Some(sender_opt) = vec.pop() {
                if let Some(sender) = sender_opt {
                    sender.send(()).unwrap();
                }
            }
        }
    }
}

impl Drop for FsyncThread {
    fn drop(&mut self) {
        self.context.shutting_down.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap_or_log();
        }
    }
}

#[cfg(test)]
mod test {
    use assert as assert_true;
    use itertools::Itertools;
    use tempdir::TempDir;

    use super::WAL;
    use crate::{DurabilityRecordType, DurabilitySequenceNumber, DurabilityService, RawRecord};
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    struct TestRecord {
        bytes: [u8; 4],
    }

    impl TestRecord {
        const RECORD_TYPE: DurabilityRecordType = 0;
        const RECORD_NAME: &'static str = "TEST";

        fn new(bytes: &[u8]) -> Self {
            Self { bytes: bytes.try_into().unwrap() }
        }

        fn bytes(&self) -> &[u8] {
            &self.bytes
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    struct UnsequencedTestRecord {
        bytes: [u8; 4],
    }

    impl UnsequencedTestRecord {
        const RECORD_TYPE: DurabilityRecordType = 1;
        const RECORD_NAME: &'static str = "UNSEQUENCED_TEST";

        fn bytes(&self) -> &[u8] {
            &self.bytes
        }
    }

    fn create_wal(directory: &TempDir) -> WAL {
        let mut wal = WAL::create(directory).unwrap();
        wal.register_record_type(TestRecord::RECORD_TYPE, TestRecord::RECORD_NAME);
        wal.register_record_type(UnsequencedTestRecord::RECORD_TYPE, UnsequencedTestRecord::RECORD_NAME);
        wal
    }

    fn load_wal(directory: &TempDir) -> WAL {
        let mut wal = WAL::load(directory).unwrap();
        wal.register_record_type(TestRecord::RECORD_TYPE, TestRecord::RECORD_NAME);
        wal.register_record_type(UnsequencedTestRecord::RECORD_TYPE, UnsequencedTestRecord::RECORD_NAME);
        wal
    }

    #[test]
    fn test_wal_write_read() {
        let directory = TempDir::new("wal-test").unwrap();

        let record = TestRecord { bytes: *b"test" };

        let wal = create_wal(&directory);
        wal.sequenced_write(TestRecord::RECORD_TYPE, record.bytes()).unwrap();

        let RawRecord { record_type, bytes, .. } =
            wal.iter_any_from(DurabilitySequenceNumber::MIN).unwrap().next().unwrap().unwrap();
        assert_eq!(record_type, TestRecord::RECORD_TYPE);

        let read_record = TestRecord::new(&bytes);
        assert_eq!(record, read_record);
    }

    #[test]
    fn test_wal_write_read_lots() {
        let directory = TempDir::new("wal-test").unwrap();

        let records = [TestRecord { bytes: *b"test" }; 1024];

        let wal = create_wal(&directory);
        records
            .iter()
            .try_for_each(|record| wal.sequenced_write(TestRecord::RECORD_TYPE, record.bytes()).map(|_| ()))
            .unwrap();

        let read_records = wal
            .iter_any_from(DurabilitySequenceNumber::MIN)
            .unwrap()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res.unwrap();
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                TestRecord::new(&bytes)
            })
            .collect_vec();

        assert_eq!(records.len(), read_records.len());
        assert_eq!(records, &*read_records);
    }

    #[test]
    fn test_wal_load() {
        let directory = TempDir::new("wal-test").unwrap();

        let record = TestRecord { bytes: *b"test" };

        let wal = create_wal(&directory);
        wal.sequenced_write(TestRecord::RECORD_TYPE, record.bytes()).unwrap();
        drop(wal);

        let wal = load_wal(&directory);
        let RawRecord { record_type, bytes, .. } =
            wal.iter_any_from(DurabilitySequenceNumber::MIN).unwrap().next().unwrap().unwrap();
        assert_eq!(record_type, TestRecord::RECORD_TYPE);

        let read_record = TestRecord::new(&bytes);
        assert_eq!(record, read_record);
    }

    #[test]
    fn test_wal_open_multiple() {
        let directory = TempDir::new("wal-test").unwrap();

        let records = [TestRecord { bytes: *b"test" }, TestRecord { bytes: *b"abcd" }];

        let wal = create_wal(&directory);
        records
            .iter()
            .try_for_each(|record| wal.sequenced_write(TestRecord::RECORD_TYPE, record.bytes()).map(|_| ()))
            .unwrap();
        drop(wal);

        let wal = load_wal(&directory);
        let read_records = wal
            .iter_any_from(DurabilitySequenceNumber::MIN)
            .unwrap()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res.unwrap();
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                TestRecord::new(&bytes)
            })
            .collect_vec();

        assert_eq!(records, &*read_records);
    }

    #[test]
    fn test_wal_iterate_from() {
        let directory = TempDir::new("wal-test").unwrap();

        let records = [TestRecord { bytes: *b"test" }, TestRecord { bytes: *b"abcd" }];

        let wal = create_wal(&directory);
        let sequence_numbers: Vec<_> = records
            .iter()
            .map(|record| wal.sequenced_write(TestRecord::RECORD_TYPE, record.bytes()))
            .try_collect()
            .unwrap();
        let iter_start = sequence_numbers[1];

        let read_records: Vec<TestRecord> = wal
            .iter_any_from(iter_start)
            .unwrap()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res.unwrap();
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                TestRecord::new(&bytes)
            })
            .collect_vec();
        assert_eq!(&records[1..], &*read_records);

        drop(wal);

        let wal = load_wal(&directory);
        let read_records = wal
            .iter_any_from(iter_start)
            .unwrap()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res.unwrap();
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                TestRecord::new(&bytes)
            })
            .collect_vec();
        assert_eq!(&records[1..], &*read_records);

        let wal = load_wal(&directory);
        let read_records =
            wal.iter_any_from(DurabilitySequenceNumber::MAX).unwrap().map(|res| res.unwrap()).collect_vec();
        assert_true!(read_records.is_empty());
    }

    #[test]
    fn test_wal_find_last() {
        let directory = TempDir::new("wal-test").unwrap();

        let sequenced_1 = TestRecord { bytes: *b"test" };
        let sequenced_2 = TestRecord { bytes: *b"abcd" };
        let unsequenced_1 = UnsequencedTestRecord { bytes: *b"unsq" };
        let unsequenced_2 = UnsequencedTestRecord { bytes: *b"xyzp" };

        let wal = create_wal(&directory);
        wal.sequenced_write(TestRecord::RECORD_TYPE, sequenced_1.bytes()).unwrap();
        wal.unsequenced_write(UnsequencedTestRecord::RECORD_TYPE, unsequenced_1.bytes()).unwrap();
        wal.unsequenced_write(UnsequencedTestRecord::RECORD_TYPE, unsequenced_2.bytes()).unwrap();
        wal.sequenced_write(TestRecord::RECORD_TYPE, sequenced_2.bytes()).unwrap();

        let found = wal.find_last_type(UnsequencedTestRecord::RECORD_TYPE).unwrap().unwrap();
        assert_true!(
            matches!(found, RawRecord { bytes, record_type: UnsequencedTestRecord::RECORD_TYPE, .. } if bytes == unsequenced_2.bytes())
        );

        drop(wal);

        let wal = load_wal(&directory);

        let found = wal.find_last_type(UnsequencedTestRecord::RECORD_TYPE).unwrap().unwrap();
        assert_true!(
            matches!(found, RawRecord { bytes, record_type: UnsequencedTestRecord::RECORD_TYPE, .. } if bytes == unsequenced_2.bytes())
        );
    }
}

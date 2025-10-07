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

#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, String>,
    next_sequence_number: AtomicU64,
    files: Arc<RwLock<Files>>,
    fsync_thread: FsyncThread,
}

impl WAL {
    pub const WAL_DIR_NAME: &'static str = "wal";

    pub fn create(directory: impl AsRef<Path>) -> Result<Self, DurabilityServiceError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if wal_dir.exists() {
            Err(WALError::CreateDirectoryExists { directory: wal_dir.clone() })?
        } else {
            fs::create_dir_all(wal_dir.clone()).map_err(|err| WALError::Create { source: Arc::new(err) })?;
        }

        let files = Files::open(wal_dir.clone())?;

        let files = Arc::new(RwLock::new(files));
        let next = RecordIterator::new(files.read().unwrap(), DurabilitySequenceNumber::MIN)?
            .last()
            .map(|rr| rr.unwrap().sequence_number.next())
            .unwrap_or(DurabilitySequenceNumber::MIN.next());
        let mut fsync_thread = FsyncThread::new(files.clone());
        FsyncThread::start(&mut fsync_thread.handle, fsync_thread.context.clone());
        Ok(Self {
            registered_types: HashMap::new(),
            next_sequence_number: AtomicU64::new(next.number()),
            files,
            fsync_thread,
        })
    }

    pub fn load(directory: impl AsRef<Path>) -> Result<Self, DurabilityServiceError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if !wal_dir.exists() {
            Err(WALError::LoadDirectoryMissing { directory: wal_dir.clone() })?
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
        Ok(Self {
            registered_types: HashMap::new(),
            next_sequence_number: AtomicU64::new(next.number()),
            files,
            fsync_thread,
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

    fn sequenced_write(
        &self,
        record_type: DurabilityRecordType,
        bytes: &[u8],
    ) -> Result<DurabilitySequenceNumber, DurabilityServiceError> {
        debug_assert!(self.registered_types.contains_key(&record_type));
        let mut files = self.files.write().unwrap();
        let seq = self.increment();
        let raw_record = RawRecord { sequence_number: seq, record_type, bytes: Cow::Borrowed(bytes) };
        files.write_record(raw_record)?;
        Ok(seq)
    }

    fn unsequenced_write(&self, record_type: DurabilityRecordType, bytes: &[u8]) -> Result<(), DurabilityServiceError> {
        debug_assert!(self.registered_types.contains_key(&record_type));
        let mut files = self.files.write().unwrap();
        let raw_record = RawRecord { sequence_number: self.previous(), record_type, bytes: Cow::Borrowed(bytes) };
        files.write_record(raw_record)?;
        Ok(())
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

    fn truncate_from(&self, sequence_number: DurabilitySequenceNumber) -> Result<(), DurabilityServiceError> {
        let mut files = self.files.write().unwrap();
        files.truncate_from(sequence_number)?;
        files.sync_all()?;
        self.next_sequence_number.store(sequence_number.number(), Ordering::SeqCst);
        Ok(())
    }

    fn delete_durability(self) -> Result<(), DurabilityServiceError> {
        drop(self.fsync_thread);
        let files = Arc::into_inner(self.files)
            .expect("cannot get exclusive ownership of WAL's Arc<Files>")
            .into_inner()
            .unwrap();
        files.delete()
    }

    fn reset(&mut self) -> Result<(), DurabilityServiceError> {
        self.next_sequence_number.store(DurabilitySequenceNumber::MIN.next().number(), Ordering::SeqCst);
        self.files.write().unwrap().reset()
    }
}

#[derive(Debug, Clone)]
pub enum WALError {
    Create { source: Arc<io::Error> },
    CreateDirectoryExists { directory: PathBuf },
    Load { source: Arc<io::Error> },
    LoadDirectoryMissing { directory: PathBuf },
    Compression { source: Arc<io::Error> },
    Decompression { source: Arc<io::Error> },
    Sync { source: Arc<io::Error> },
}

impl fmt::Display for WALError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for WALError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Create { source, .. } => Some(source),
            Self::CreateDirectoryExists { .. } => None,
            Self::Load { source, .. } => Some(source),
            Self::LoadDirectoryMissing { .. } => None,
            Self::Compression { source, .. } => Some(source),
            Self::Decompression { source, .. } => Some(source),
            Self::Sync { source, .. } => Some(source),
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
            last.trim_corrupted_tail_if_needed()?;
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

    fn write_record(&mut self, record: RawRecord<'_>) -> Result<(), DurabilityServiceError> {
        if self.files.is_empty() || self.files.last().unwrap().len >= MAX_WAL_FILE_SIZE {
            self.open_new_file_at(record.sequence_number)?;
        }

        let mut compressed_bytes = Vec::new();
        let mut encoder = lz4::EncoderBuilder::new()
            .build(&mut compressed_bytes)
            .map_err(|err| WALError::Compression { source: Arc::new(err) })?;
        encoder.write_all(&record.bytes).map_err(|err| WALError::Compression { source: Arc::new(err) })?;
        encoder.finish().1.map_err(|err| WALError::Compression { source: Arc::new(err) })?;

        let writer = self.writer.as_mut().unwrap();
        write_header(
            writer,
            RecordHeader {
                sequence_number: record.sequence_number,
                len: compressed_bytes.len() as u64,
                record_type: record.record_type,
            },
        )?;

        writer.write_all(&compressed_bytes)?;
        writer.flush()?;

        self.files.last_mut().unwrap().len = writer.stream_position()?;
        Ok(())
    }

    pub(crate) fn sync_all(&mut self) -> Result<(), DurabilityServiceError> {
        self.files
            .last_mut()
            .expect("Expected at least one file")
            .writer()
            .expect("Expected file writer on sync all")
            .get_mut()
            .sync_all()
            .map_err(|err| WALError::Sync { source: Arc::new(err) })?;
        self.sync_directory_best_effort()
    }

    fn sync_directory_best_effort(&mut self) -> Result<(), DurabilityServiceError> {
        #[cfg(unix)]
        {
            StdFile::open(&self.directory)
                .map_err(|err| WALError::Sync { source: Arc::new(err) })?
                .sync_all()
                .map_err(|err| WALError::Sync { source: Arc::new(err) }.into())
        }

        #[cfg(windows)]
        {
            // On Windows, FlushFileBuffers doesn't support directory handles, so it's likely
            // a noop or an error (which is ignored), but we try it for symmetry.
            // TODO: This requires additional testing and probably a separate OS-specific impl.
            if let Ok(dir) = StdFile::open(&self.directory) {
                let _ = dir.sync_all();
            }
            Ok(())
        }
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = &File> {
        self.files.iter()
    }

    fn file_index_containing(&self, sequence_number: DurabilitySequenceNumber) -> Option<usize> {
        self.files.iter().rposition(|f| f.start.number() <= sequence_number.number())
    }

    fn truncate_from(&mut self, sequence_number: DurabilitySequenceNumber) -> Result<(), DurabilityServiceError> {
        let Some(file_index) = self.file_index_containing(sequence_number) else {
            return Ok(());
        };

        // Call this before file deletion so we don't delete files in case of an error.
        let Some(truncate_position) = self.files[file_index].offset_of(sequence_number)? else {
            // Already does not have anything from this sequence number. Can be changed to an error.
            return Ok(());
        };

        while self.files.len() > file_index + 1 {
            fs::remove_file(&self.files.pop().unwrap().path)?;
        }

        let last = &mut self.files[file_index];
        last.truncate_from_position(truncate_position)?;
        self.writer = Some(last.writer()?);
        Ok(())
    }

    fn delete(self) -> Result<(), DurabilityServiceError> {
        drop(self.files);
        fs::remove_dir_all(&self.directory).map_err(|source| source.into())
    }

    fn reset(&mut self) -> Result<(), DurabilityServiceError> {
        fs::remove_dir_all(&self.directory)?;
        fs::create_dir(&self.directory)?;
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

    fn trim_corrupted_tail_if_needed(&mut self) -> Result<(), DurabilityServiceError> {
        let mut reader = FileReader::new(self.clone())?;
        let mut last_good_position_end = 0;
        while let Some(record) = reader.read_one_record().transpose() {
            if record.as_ref().is_ok_and(|record| !record.bytes.is_empty()) {
                last_good_position_end = reader.reader.stream_position()?;
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
                self.truncate_from_position(last_good_position_end)?;
                break;
            }
        }

        Ok(())
    }

    fn offset_of(&self, sequence_number: DurabilitySequenceNumber) -> Result<Option<u64>, DurabilityServiceError> {
        let mut reader = FileReader::new(self.clone())?;
        let mut current_record_offset = 0;

        while let Some(record) = reader.read_one_record()? {
            if record.sequence_number.number() == sequence_number.number() {
                return Ok(Some(current_record_offset));
            }
            // Points to the beginning of the next record
            current_record_offset = reader.reader.stream_position()?;
        }

        Ok(None)
    }

    fn truncate_from_position(&mut self, position: u64) -> Result<(), DurabilityServiceError> {
        OpenOptions::new().write(true).open(&self.path)?.set_len(position)?;
        self.len = position;
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
            context.files.write().unwrap().sync_all().expect("Expected sync all");
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

    use super::{MAX_WAL_FILE_SIZE, WAL};
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

    fn read_all_records(wal: &WAL) -> impl Iterator<Item = RawRecord<'_>> {
        wal.iter_any_from(DurabilitySequenceNumber::MIN).unwrap().map(|res| res.unwrap())
    }

    fn read_all_records_tupled(wal: &WAL) -> Vec<(DurabilitySequenceNumber, DurabilityRecordType, Vec<u8>)> {
        read_all_records(wal)
            .map(|res| {
                let RawRecord { sequence_number, record_type, bytes } = res;
                (sequence_number, record_type, bytes.into_owned())
            })
            .collect_vec()
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

    #[test]
    fn test_wal_truncate_from_middle_of_single_file_and_continue() {
        let directory = TempDir::new("wal-test").unwrap();
        let wal = create_wal(&directory);

        let records = [b"a000", b"b111", b"c222", b"d333", b"e444"];
        let seqs: Vec<_> = records
            .iter()
            .map(|record| wal.sequenced_write(TestRecord::RECORD_TYPE, record.as_ref()))
            .try_collect()
            .unwrap();

        let reads_before_cut = read_all_records_tupled(&wal);
        assert_eq!(reads_before_cut.len(), 5);

        let cut = seqs[2];
        wal.truncate_from(cut).expect("Expected to truncate everything starting from seqs[2] (including itself)");

        assert_eq!(wal.current(), seqs[2], "Expected to have the current seq equal to the cut seq");

        let reads_after_cut = read_all_records_tupled(&wal);

        assert_eq!(
            reads_after_cut,
            reads_before_cut[..2].to_vec(),
            "Expected only two records after the cut, without the truncated and following records"
        );

        assert_eq!(wal.current(), cut);
        let new_seq1 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"x555").unwrap();
        assert_eq!(new_seq1, cut, "Expected to have the next seq equal to the cut seq");

        let new_seq2 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"y666").unwrap();
        assert_eq!(new_seq2, cut.next(), "Expected to have the next next seq equal to the cut's next seq");

        let reads_after_new_writes = read_all_records_tupled(&wal);
        assert_eq!(
            reads_after_new_writes,
            vec![
                reads_before_cut[0].clone(),
                reads_before_cut[1].clone(),
                (new_seq1, TestRecord::RECORD_TYPE, b"x555".to_vec()),
                (new_seq2, TestRecord::RECORD_TYPE, b"y666".to_vec()),
            ]
        );

        // Verify the same after reload.
        drop(wal);
        let wal = load_wal(&directory);
        let reads_reloaded = read_all_records_tupled(&wal);
        assert_eq!(reads_reloaded, reads_after_new_writes);

        let new_seq3 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"z777").unwrap();
        assert_eq!(new_seq3, cut.next().next(), "Expected the final seq to be the cut's next next one");

        let reads_final = read_all_records_tupled(&wal);
        assert_eq!(
            reads_final,
            reads_after_new_writes
                .into_iter()
                .chain(std::iter::once((new_seq3, TestRecord::RECORD_TYPE, b"z777".to_vec())))
                .collect_vec()
        );
    }

    #[test]
    fn test_wal_truncate_from_across_multiple_files_deletes_newer_files() {
        let directory = TempDir::new("wal-test").unwrap();
        let wal = create_wal(&directory);

        let mut seqs = Vec::new();
        // Should be enough for 3 files
        let records_num = MAX_WAL_FILE_SIZE.div_ceil(16) as usize;
        for i in 0..records_num {
            let payload = format!("r{:04}", i);
            seqs.push(wal.sequenced_write(TestRecord::RECORD_TYPE, payload.as_bytes()).unwrap());
        }

        let cut = seqs[records_num.div_ceil(2)];
        wal.truncate_from(cut).unwrap();

        let reads_before = read_all_records(&wal).map(|record| record.sequence_number).collect_vec();
        assert!(!reads_before.is_empty());
        assert!(reads_before.iter().all(|s| s.number() < cut.number()));
        assert_eq!(wal.current(), cut);

        drop(wal);
        let wal = load_wal(&directory);
        let reads_after_reload = read_all_records(&wal).map(|record| record.sequence_number).collect_vec();
        assert_eq!(reads_before, reads_after_reload);
        assert_eq!(wal.current(), cut);
    }

    #[test]
    fn test_wal_truncate_from_beginning_clears_everything() {
        let directory = TempDir::new("wal-test").unwrap();
        let wal = create_wal(&directory);

        let s1 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"one!").unwrap();
        let _s2 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"two!").unwrap();

        wal.truncate_from(s1).unwrap();

        let read_records = read_all_records(&wal).collect_vec();
        assert!(read_records.is_empty(), "expected no records after truncate_from(first)");
        assert_eq!(wal.current(), s1);
    }

    #[test]
    fn test_wal_truncate_from_is_idempotent_for_same_cut() {
        let directory = TempDir::new("wal-test").unwrap();
        let wal = create_wal(&directory);

        let _s1 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"one!").unwrap();
        let s2 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"two!").unwrap();

        wal.truncate_from(s2).unwrap();
        wal.truncate_from(s2).unwrap();

        let read_records = read_all_records(&wal).map(|record| record.bytes.into_owned()).collect_vec();
        assert_eq!(read_records, vec![b"one!".to_vec()]);
        assert_eq!(wal.current(), s2);
    }

    #[test]
    fn truncate_from_keeps_prior_unsequenced_records_with_same_seq() {
        let directory = TempDir::new("wal-test").unwrap();
        let wal = create_wal(&directory);

        let s1 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"S111").unwrap();
        wal.unsequenced_write(UnsequencedTestRecord::RECORD_TYPE, b"UXXX").unwrap();
        wal.unsequenced_write(UnsequencedTestRecord::RECORD_TYPE, b"UYYY").unwrap();
        let s2 = wal.sequenced_write(TestRecord::RECORD_TYPE, b"S222").unwrap();
        wal.unsequenced_write(UnsequencedTestRecord::RECORD_TYPE, b"UZZZ").unwrap();

        let reads_before_cut = read_all_records_tupled(&wal);
        assert_eq!(reads_before_cut.len(), 5, "Expected 5 records before truncation");

        wal.truncate_from(s2).expect("Expected to cut at s2 to remove S222 and UZZZ");
        assert_eq!(wal.current(), s2, "Expected current to be equal to the cut seq");

        let reads_after_cut = read_all_records_tupled(&wal);
        assert_eq!(
            reads_after_cut,
            vec![
                (s1, TestRecord::RECORD_TYPE, b"S111".to_vec()),
                (s1, UnsequencedTestRecord::RECORD_TYPE, b"UXXX".to_vec()),
                (s1, UnsequencedTestRecord::RECORD_TYPE, b"UYYY".to_vec()),
            ]
        );

        let found_last_unseq = wal.find_last_type(UnsequencedTestRecord::RECORD_TYPE).unwrap().unwrap();
        assert_eq!(found_last_unseq.bytes.into_owned(), b"UYYY");
    }
}

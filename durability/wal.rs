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
    ops::Sub,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock, RwLockReadGuard,
    },
};

use itertools::Itertools;

use crate::{DurabilityRecordType, DurabilitySequenceNumber, DurabilityService, DurabilityServiceError, RawRecord};

const MAX_WAL_FILE_SIZE: u64 = 16 * 1024 * 1024;

const FILE_PREFIX: &str = "wal-";

#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, String>,
    next_sequence_number: AtomicU64,
    files: RwLock<Files>,
}

impl WAL {
    pub const WAL_DIR_NAME: &'static str = "wal";

    pub fn create(directory: impl AsRef<Path>) -> Result<Self, WALError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if wal_dir.exists() {
            Err(WALError::CreateErrorDirectoryExists { directory: wal_dir.clone() })?
        } else {
            fs::create_dir_all(wal_dir.clone()).map_err(|err| WALError::CreateError { source: err })?;
        }

        let files = Files::open(wal_dir.clone()).map_err(|err| WALError::CreateError { source: err })?;

        let files = RwLock::new(files);
        let next = RecordIterator::new(files.read().unwrap(), DurabilitySequenceNumber::MIN)
            .map_err(|err| WALError::CreateError { source: err })?
            .last()
            .map(|rr| rr.unwrap().sequence_number.next())
            .unwrap_or(DurabilitySequenceNumber::MIN.next());
        Ok(Self { registered_types: HashMap::new(), next_sequence_number: AtomicU64::new(next.number()), files })
    }

    pub fn load(directory: impl AsRef<Path>) -> Result<Self, WALError> {
        let directory = directory.as_ref().to_owned();
        let wal_dir = directory.join(Self::WAL_DIR_NAME);
        if !wal_dir.exists() {
            Err(WALError::LoadErrorDirectoryMissing { directory: wal_dir.clone() })?
        }

        let files = Files::open(wal_dir.clone()).map_err(|err| WALError::LoadError { source: err })?;

        let files = RwLock::new(files);
        let next = RecordIterator::new(files.read().unwrap(), DurabilitySequenceNumber::MIN)
            .map_err(|err| WALError::LoadError { source: err })?
            .last()
            .map(|rr| rr.unwrap().sequence_number.next())
            .unwrap_or(DurabilitySequenceNumber::MIN.next());
        Ok(Self { registered_types: HashMap::new(), next_sequence_number: AtomicU64::new(next.number()), files })
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
        let mut files = self.files.write().unwrap();
        let raw_record = RawRecord { sequence_number: self.previous(), record_type, bytes: Cow::Borrowed(bytes) };
        files.write_record(raw_record)?;
        Ok(())
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

    fn iter_any_from(
        &self,
        sequence_number: DurabilitySequenceNumber,
    ) -> Result<impl Iterator<Item = Result<RawRecord<'static>, DurabilityServiceError>>, DurabilityServiceError> {
        Ok(RecordIterator::new(self.files.read().unwrap(), sequence_number)?)
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
            let iterator = FileRecordIterator::new(file, DurabilitySequenceNumber::MIN)
                .map_err(|err| DurabilityServiceError::IO { source: err })?;

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
        let files = self.files.into_inner().unwrap();
        files.delete().map_err(|err| DurabilityServiceError::DeleteFailed { source: err })
    }
}

#[derive(Debug)]
pub enum WALError {
    CreateError { source: io::Error },
    CreateErrorDirectoryExists { directory: PathBuf },
    LoadError { source: io::Error },
    LoadErrorDirectoryMissing { directory: PathBuf },
}

impl fmt::Display for WALError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for WALError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateError { source, .. } => Some(source),
            Self::CreateErrorDirectoryExists { .. } => None,
            Self::LoadError { source, .. } => Some(source),
            Self::LoadErrorDirectoryMissing { .. } => None,
        }
    }
}

#[derive(Debug)]
struct Files {
    directory: PathBuf,
    writer: BufWriter<StdFile>,
    files: Vec<File>,
}

impl Files {
    fn open(directory: PathBuf) -> io::Result<Self> {
        let mut files: Vec<File> = directory
            .read_dir()?
            .map_ok(|entry| entry.path())
            .filter_ok(|path| {
                path.file_name().and_then(OsStr::to_str).is_some_and(|name| name.starts_with(FILE_PREFIX))
            })
            .map(|path| File::open(path?))
            .try_collect()?;
        files.sort_unstable_by(|lhs, rhs| lhs.path.cmp(&rhs.path));

        if files.is_empty() {
            files.push(File::open_at(directory.clone(), DurabilitySequenceNumber::MIN.next())?);
        }

        let writer = files.last().unwrap().writer()?;

        Ok(Self { directory, writer, files })
    }

    fn open_new_file_at(&mut self, start: DurabilitySequenceNumber) -> io::Result<()> {
        let file = File::open_at(self.directory.clone(), start)?;
        self.writer = file.writer()?;
        self.files.push(file);
        Ok(())
    }

    fn write_record(&mut self, record: RawRecord<'_>) -> Result<(), DurabilityServiceError> {
        if self.files.last().unwrap().len >= MAX_WAL_FILE_SIZE {
            self.open_new_file_at(record.sequence_number)?;
        }
        write_header(
            &mut self.writer,
            RecordHeader {
                sequence_number: record.sequence_number,
                len: record.bytes.len() as u64,
                record_type: record.record_type,
            },
        )?;

        self.writer.write_all(&record.bytes)?;
        self.writer.flush()?;

        self.files.last_mut().unwrap().len = self.writer.stream_position()?;
        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = &File> + DoubleEndedIterator {
        self.files.iter()
    }

    fn delete(self) -> Result<(), io::Error> {
        drop(self.files);
        std::fs::remove_dir_all(&self.directory)
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

    fn read_one_record(&mut self) -> io::Result<Option<RawRecord<'static>>> {
        if self.reader.stream_position()? == self.file.len {
            return Ok(None);
        }
        let RecordHeader { sequence_number, len, record_type } = self.read_header()?;

        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;

        Ok(Some(RawRecord { sequence_number, record_type, bytes: Cow::Owned(buf) }))
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
    fn new(files: RwLockReadGuard<'a, Files>, start: DurabilitySequenceNumber) -> io::Result<Self> {
        let (current, mut current_start) = files
            .iter()
            .map_while(|file| (file.start < start).then_some(file.start))
            .enumerate()
            .last()
            .unwrap_or((0, files.files[0].start));
        let mut reader = FileReader::new(files.files[current].clone())?;

        while current_start < start {
            reader.read_one_record()?;
            match reader.peek_sequence_number().transpose() {
                None => {
                    // sequence number is past the end of this file.
                    break;
                }
                Some(Err(err)) => return Err(err),
                Some(Ok(sequence_number)) => {
                    current_start = sequence_number;
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

impl<'a> Iterator for RecordIterator<'a> {
    type Item = Result<RawRecord<'static>, DurabilityServiceError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        match reader.read_one_record().transpose() {
            Some(Ok(item)) => Some(Ok(item)),
            Some(Err(error)) => Some(Err(DurabilityServiceError::IO { source: error })),
            None => match self.advance_file().transpose()? {
                Ok(()) => self.next(),
                Err(error) => {
                    self.reader = None;
                    Some(Err(DurabilityServiceError::IO { source: error }))
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
    fn new(file: &'a File, start: DurabilitySequenceNumber) -> io::Result<Self> {
        let mut reader = FileReader::new(file.clone())?;

        let mut current_start = file.start;
        while current_start < start {
            reader.read_one_record()?;
            match reader.peek_sequence_number().transpose() {
                None => {
                    // sequence number is past the end of this file.
                    break;
                }
                Some(Err(err)) => return Err(err),
                Some(Ok(sequence_number)) => {
                    current_start = sequence_number;
                }
            }
        }
        Ok(Self { reader: Some(reader), file_ref: PhantomData::default() })
    }
}

impl<'a> Iterator for FileRecordIterator<'a> {
    type Item = Result<RawRecord<'static>, DurabilityServiceError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        match reader.read_one_record().transpose() {
            Some(Ok(item)) => Some(Ok(item)),
            Some(Err(error)) => Some(Err(DurabilityServiceError::IO { source: error })),
            None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

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

        fn new(bytes: &[u8]) -> Self {
            Self { bytes: bytes.try_into().unwrap() }
        }

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
        assert!(read_records.is_empty());
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
        assert!(
            matches!(found, RawRecord { bytes: Cow::Owned(bytes), record_type: UnsequencedTestRecord::RECORD_TYPE, .. } if &bytes == unsequenced_2.bytes())
        );

        drop(wal);

        let wal = load_wal(&directory);

        let found = wal.find_last_type(UnsequencedTestRecord::RECORD_TYPE).unwrap().unwrap();
        assert!(
            matches!(found, RawRecord { bytes: Cow::Owned(bytes), record_type: UnsequencedTestRecord::RECORD_TYPE, .. } if &bytes == unsequenced_2.bytes())
        );
    }
}

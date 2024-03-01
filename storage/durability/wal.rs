/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, File as StdFile, OpenOptions},
    io::{self, Read, Seek, Write},
    iter,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
};

use itertools::Itertools;
use primitive::U80;

use crate::{
    DurabilityRecord, DurabilityRecordType, DurabilityService, RawRecord, RecordHeader, Result, SequenceNumber,
    Sequencer,
};

const MAX_WAL_FILE_SIZE: u64 = 1024;

const FILE_PREFIX: &str = "wal-";
const CHECKPOINTED_SUFFIX: &str = "-checkpoint";

//
// I think we could use an MMAP append-only file to allow records to serialise themselves directly into the right place
// We could also use a Writer/Stream compressor to reduce the write bandwidth requirements
//
#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, &'static str>,
    next_sequence_number: AtomicU64,
    redo_point: AtomicU64,
    directory: PathBuf,
    files: Mutex<Files>,
}

#[derive(Debug)]
struct File {
    start: SequenceNumber,
    handle: StdFile,
    len: u64,
    path: PathBuf,
}

impl File {
    fn format_file_name(seq: SequenceNumber) -> String {
        format!("{}{:025}", FILE_PREFIX, seq.number().number())
    }

    fn open_at(directory: PathBuf, start: SequenceNumber) -> io::Result<Self> {
        let path = directory.join(Self::format_file_name(start));
        let mut handle = OpenOptions::new().read(true).append(true).create(true).open(&path)?;
        let len = handle.seek(io::SeekFrom::End(0))?;
        Ok(Self { start, handle, len, path })
    }

    fn open(path: PathBuf) -> io::Result<Self> {
        let num = path.file_name().and_then(|s| s.to_str()).and_then(|s| s.split('-').nth(1)).unwrap().parse().unwrap();
        let mut handle = OpenOptions::new().read(true).append(true).create(true).open(&path)?;
        let len = handle.seek(io::SeekFrom::End(0))?;
        Ok(Self { start: SequenceNumber::new(U80::new(num)), handle, len, path })
    }
}

#[derive(Debug)]
struct Files {
    current: File,
    previous: Vec<File>,
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

        let last = files.pop().map(Ok).unwrap_or_else(|| File::open_at(directory, SequenceNumber::new(U80::new(0))))?;

        Ok(Self { current: last, previous: files })
    }

    fn open_new_file_at(&mut self, directory: PathBuf, start: SequenceNumber) -> io::Result<()> {
        let mut file = File::open_at(directory, start)?;
        std::mem::swap(&mut self.current, &mut file);
        self.previous.push(file);
        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = &File> {
        self.previous.iter().chain(iter::once(&self.current))
    }
}

struct RecordIterator<'a> {
    files: MutexGuard<'a, Files>,
    current: Option<usize>,
}

impl<'a> RecordIterator<'a> {
    fn new(files: MutexGuard<'a, Files>) -> Self {
        let mut this = Self { current: (!files.previous.is_empty()).then_some(0), files };
        this.rewind_current();
        this
    }

    fn rewind_current(&mut self) {
        self.current().handle.rewind().unwrap();
    }

    fn current(&mut self) -> &mut File {
        match self.current {
            None => &mut self.files.current,
            Some(n) => self.files.previous.get_mut(n).unwrap(),
        }
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = io::Result<RawRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let file = self.current();
        match read_one_record(file).transpose() {
            None => {
                if let Some(n) = self.current {
                    self.current = if n + 1 < self.files.previous.len() { Some(n + 1) } else { None };
                    self.rewind_current();
                    self.next()
                } else {
                    None
                }
            }
            some => some,
        }
    }
}

impl WAL {
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_impl(directory.as_ref().to_owned())
    }

    fn open_impl(directory: PathBuf) -> io::Result<Self> {
        if !directory.exists() {
            fs::create_dir_all(&directory)?;
        }

        let files = Files::open(directory.clone())?;
        let next = 0; // TODO
        let redo_point = files
            .iter()
            .find(|f| !f.path.file_name().and_then(|s| s.to_str()).unwrap().ends_with(CHECKPOINTED_SUFFIX))
            .map(|f| f.start.number().number() as u64)
            .unwrap_or(next);

        Ok(Self {
            registered_types: HashMap::new(),
            next_sequence_number: AtomicU64::new(next),
            redo_point: AtomicU64::new(redo_point),
            files: Mutex::new(files),
            directory,
        })
    }
}

impl Sequencer for WAL {
    fn increment(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.next_sequence_number.fetch_add(1, Ordering::Relaxed) as u128))
    }

    fn current(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.next_sequence_number.load(Ordering::Relaxed) as u128))
    }

    fn previous(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.next_sequence_number.load(Ordering::Relaxed) as u128 - 1))
    }
}

impl DurabilityService for WAL {
    fn register_record_type<Record: DurabilityRecord>(&mut self) {
        if self.registered_types.get(&Record::RECORD_TYPE).is_some_and(|name| name != &Record::RECORD_NAME) {
            panic!("Illegal state: two types of WAL records registered with same ID and different names.")
        }
        self.registered_types.insert(Record::RECORD_TYPE, Record::RECORD_NAME);
    }

    fn sequenced_write<Record>(&self, record: &Record) -> Result<SequenceNumber>
    where
        Record: DurabilityRecord,
    {
        debug_assert!(self.registered_types.get(&Record::RECORD_TYPE) == Some(&Record::RECORD_NAME));
        let mut files = self.files.lock().unwrap();

        let file = &mut files.current;
        file.handle.seek(io::SeekFrom::End(0))?;
        let seq = self.increment();

        let mut buf = Vec::new();
        record.serialise_into(&mut buf)?;

        write_header::<Record>(&mut file.handle, seq, buf.len() as u32)?;
        file.handle.write_all(&buf)?;

        file.len = file.handle.stream_position()?;

        if file.len >= MAX_WAL_FILE_SIZE {
            files.open_new_file_at(self.directory.clone(), self.current())?;
        }

        Ok(seq)
    }

    fn iter_from(&self, sequence_number: SequenceNumber) -> impl Iterator<Item = io::Result<RawRecord>> {
        let files = self.files.lock().unwrap();
        RecordIterator::new(files).skip_while(move |r| r.as_ref().is_ok_and(|r| r.sequence_number < sequence_number))
    }

    fn checkpoint(&self) -> Result<()> {
        let mut files = self.files.lock().unwrap();

        let checkpointed_path = files.current.path.with_file_name(
            files.current.path.file_name().and_then(|s| s.to_str()).unwrap().to_owned() + CHECKPOINTED_SUFFIX,
        );
        fs::rename(&files.current.path, &checkpointed_path)?;
        files.current.handle = OpenOptions::new().read(true).append(true).create(true).open(&checkpointed_path)?;
        files.current.path = checkpointed_path;

        let next = self.current();
        files.open_new_file_at(self.directory.clone(), next)?;
        self.redo_point.store(self.next_sequence_number.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(())
    }

    fn recover(&self) -> impl Iterator<Item = io::Result<RawRecord>> {
        self.iter_from(SequenceNumber::new(U80::new(self.redo_point.load(Ordering::Relaxed) as u128)))
    }
}

fn write_header<Record: DurabilityRecord>(file: &mut StdFile, seq: SequenceNumber, len: u32) -> io::Result<()> {
    file.write_all(&seq.to_be_bytes())?;
    file.write_all(&len.to_be_bytes())?;
    file.write_all(&[Record::RECORD_TYPE])?;
    Ok(())
}

fn read_one_record(file: &mut File) -> io::Result<Option<RawRecord>> {
    if file.handle.stream_position()? == file.len {
        return Ok(None);
    }
    let RecordHeader { sequence_number, len, record_type } = read_header(&mut file.handle)?;
    let mut bytes = vec![0; len as usize].into_boxed_slice();
    file.handle.read_exact(&mut bytes)?;
    Ok(Some(RawRecord { sequence_number, record_type, bytes }))
}

fn read_header(file: &mut StdFile) -> io::Result<RecordHeader> {
    let mut buf = [0; U80::BYTES];
    file.read_exact(&mut buf)?;
    let sequence_number = SequenceNumber::new(U80::from_be_bytes(&buf));

    let mut buf = [0; std::mem::size_of::<u32>()];
    file.read_exact(&mut buf)?;
    let len = u32::from_be_bytes(buf);

    let mut buf = [0; 1];
    file.read_exact(&mut buf)?;
    let [record_type] = buf;

    Ok(RecordHeader { sequence_number, len, record_type })
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::WAL;
    use crate::{DurabilityRecord, DurabilityRecordType, DurabilityService, RawRecord};

    type BoxError = Box<dyn std::error::Error>;

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    struct TestRecord {
        bytes: [u8; 4],
    }

    impl DurabilityRecord for TestRecord {
        const RECORD_TYPE: DurabilityRecordType = 0;
        const RECORD_NAME: &'static str = "TEST";

        fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
            writer.write_all(&self.bytes)?;
            Ok(())
        }

        fn deserialize_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
            let mut bytes = [0; 4];
            reader.read_exact(&mut bytes)?;
            Ok(Self { bytes })
        }
    }

    #[test]
    fn test_wal_write_read() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let record = TestRecord { bytes: *b"test" };

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        wal.sequenced_write(&record)?;

        let RawRecord { record_type, bytes, .. } = wal.recover().next().unwrap()?;
        assert_eq!(record_type, TestRecord::RECORD_TYPE);

        let read_record = TestRecord::deserialize_from(&mut &*bytes)?;
        assert_eq!(record, read_record);

        Ok(())
    }

    #[test]
    fn test_wal_write_read_lots() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let records = [TestRecord { bytes: *b"test" }; 1024];

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        records.iter().try_for_each(|record| wal.sequenced_write(record).map(|_| ()))?;

        let read_records = wal
            .recover()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;

        assert_eq!(records.len(), read_records.len());
        assert_eq!(records, &*read_records);

        Ok(())
    }

    #[test]
    fn test_wal_recover() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let record = TestRecord { bytes: *b"test" };

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        wal.sequenced_write(&record)?;
        drop(wal);

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();

        let RawRecord { record_type, bytes, .. } = wal.recover().next().unwrap()?;
        assert_eq!(record_type, TestRecord::RECORD_TYPE);

        let read_record = TestRecord::deserialize_from(&mut &*bytes)?;
        assert_eq!(record, read_record);

        Ok(())
    }

    #[test]
    fn test_wal_recover_multiple() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let records = [TestRecord { bytes: *b"test" }, TestRecord { bytes: *b"abcd" }];

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        records.iter().try_for_each(|record| wal.sequenced_write(record).map(|_| ()))?;
        drop(wal);

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();

        let read_records = wal
            .recover()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;

        assert_eq!(records, &*read_records);

        Ok(())
    }

    #[test]
    fn test_wal_checkpoint_recover() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let committed_record = TestRecord { bytes: *b"1234" };
        let records = [TestRecord { bytes: *b"test" }, TestRecord { bytes: *b"abcd" }];

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        wal.sequenced_write(&committed_record)?;
        wal.checkpoint()?;
        records.iter().try_for_each(|record| wal.sequenced_write(record).map(|_| ()))?;

        let read_records = wal
            .recover()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;

        assert_eq!(records, &*read_records);

        drop(wal);

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();

        let read_records = wal
            .recover()
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;

        assert_eq!(records, &*read_records);

        Ok(())
    }

    #[test]
    fn test_wal_iterate_from() -> Result<(), BoxError> {
        let directory = tempdir::TempDir::new("wal-test")?;

        let records = [TestRecord { bytes: *b"test" }, TestRecord { bytes: *b"abcd" }];

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        let sequence_numbers: Vec<_> = records.iter().map(|record| wal.sequenced_write(record)).try_collect()?;
        let iter_start = sequence_numbers[1];

        let read_records = wal
            .iter_from(iter_start)
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;
        assert_eq!(&records[1..], &*read_records);

        drop(wal);

        let mut wal = WAL::open(&directory)?;
        wal.register_record_type::<TestRecord>();
        let read_records = wal
            .iter_from(iter_start)
            .map(|res| {
                let RawRecord { record_type, bytes, .. } = res?;
                assert_eq!(record_type, TestRecord::RECORD_TYPE);
                Ok::<_, BoxError>(TestRecord::deserialize_from(&mut &*bytes)?)
            })
            .try_collect::<_, Vec<TestRecord>, _>()?;
        assert_eq!(&records[1..], &*read_records);

        Ok(())
    }
}

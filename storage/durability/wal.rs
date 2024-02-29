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
    io::{self, Read, Write},
    iter,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use itertools::Itertools;
use primitive::U80;

use crate::{DurabilityRecord, DurabilityRecordType, DurabilityService, RawRecord, Result, SequenceNumber, Sequencer};

//
// I think we could use an MMAP append-only file to allow records to serialise themselves directly into the right place
// We could also use a Writer/Stream compressor to reduce the write bandwidth requirements
//
#[derive(Debug)]
pub struct WAL {
    registered_types: HashMap<DurabilityRecordType, &'static str>,
    sequence_number: AtomicU64,
    directory: PathBuf,
    files: Mutex<Files>,
}

struct File {
    file: StdFile,
    path: PathBuf,
}

impl File {
    fn open(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).append(true).create(true).open(&path)?;
        Ok(Self { file, path })
    }
}

struct Files {
    current: File,
    previous: Vec<File>,
}

const FILE_PREFIX: &str = "wal-";

impl Files {
    fn format_file_name(seq: SequenceNumber) -> String {
        format!("{}{:020}", FILE_PREFIX, seq.number().number())
    }

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

        let seq = SequenceNumber::new(U80::new(0));
        let last = files.pop().map(Ok).unwrap_or_else(|| File::open(directory.join(Self::format_file_name(seq))))?;

        Ok(Self { current: last, previous: files })
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

        Ok(Self {
            registered_types: HashMap::new(),
            sequence_number: AtomicU64::new(1),
            files: Mutex::new(Files::open(directory.clone())?),
            directory,
        })
    }

    fn write_header<Record: DurabilityRecord>(file: &mut StdFile, seq: SequenceNumber, len: u32) -> io::Result<()> {
        file.write_all(&seq.number().to_be_bytes())?;
        file.write_all(&len.to_be_bytes())?;
        file.write_all(&[Record::RECORD_TYPE])?;
        Ok(())
    }
}

impl Sequencer for WAL {
    fn increment(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.fetch_add(1, Ordering::Relaxed) as u128))
    }

    fn current(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.load(Ordering::Relaxed) as u128))
    }

    fn previous(&self) -> SequenceNumber {
        SequenceNumber::new(U80::new(self.sequence_number.load(Ordering::Relaxed) as u128 - 1))
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

        let file = &mut files.current.file;
        let seq = self.increment();

        let mut buf = Vec::new();
        record.serialise_into(&mut buf)?;

        Self::write_header::<Record>(file, seq, buf.len() as u32).unwrap(); // TODO
        file.write_all(&buf).unwrap(); // TODO

        Ok(seq)
    }

    // fn iterate_records_from(&self, sequence_number: SequenceNumber) -> Box<dyn Iterator<Item=(SequenceNumber, DurabilityRecordType, Box<[u8]>)>> { todo!() }

    fn recover(&self) -> impl Iterator<Item = io::Result<RawRecord>> {
        let mut files = self.files.lock().unwrap();
        iter::from_fn(move || read_one_record(&mut files.current.file).transpose())
    }
}

fn read_one_record(file: &mut StdFile) -> io::Result<Option<RawRecord>> {
    let (sequence_number, len, record_type) = match read_header(file) {
        Ok(Some(header)) => header,
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(None) => return Ok(None),
        Err(err) => return Err(err),
    };

    let mut bytes = vec![0; len as usize].into_boxed_slice();
    file.read_exact(&mut bytes)?;
    Ok(Some(RawRecord { sequence_number, record_type, bytes }))
}

fn read_header(file: &mut StdFile) -> io::Result<Option<(SequenceNumber, u32, u8)>> {
    let mut buf = [0; U80::BYTES];
    match file.read_exact(&mut buf) {
        Ok(()) => (),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let seq = SequenceNumber::new(U80::from_be_bytes(&buf));
    let mut buf = [0; std::mem::size_of::<u32>()];
    file.read_exact(&mut buf)?;
    let len = u32::from_be_bytes(buf);
    let mut buf = [0; 1];
    file.read_exact(&mut buf)?;
    let [record_type] = buf;
    Ok(Some((seq, len, record_type)))
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::WAL;
    use crate::{DurabilityRecord, DurabilityRecordType, DurabilityService, RawRecord};

    type BoxError = Box<dyn std::error::Error>;

    #[derive(Debug, PartialEq, Eq)]
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
}

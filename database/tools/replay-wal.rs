/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs::create_dir_all, io, path::PathBuf};

use clap::{Parser, ValueEnum};
use concept::thing::statistics::Statistics;
use durability::{wal::WAL, DurabilitySequenceNumber};
use storage::{
    durability_client::{DurabilityClient, DurabilityRecord, WALClient},
    isolation_manager::{CommitRecord, StatusRecord},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The database directory location; the WAL is expected to be a subdirectory named 'wal' under this.
    #[arg(value_name = "SRC-DIR")]
    source_directory: PathBuf,

    /// Directory to write the filtered WAL to; if it or <TGT-DIR>/wal does not exist, it will be created
    #[arg(value_name = "TGT-DIR")]
    target_directory: PathBuf,

    /// Kinds of records to keep
    #[arg(short, long)]
    kind: Vec<RecordKind>,

    /// The sequence number to start at (inclusive)
    #[arg(short, long, default_value = "0")]
    from: u64,

    /// The sequence number to end at (inclusive) [default: last in the WAL]
    #[arg(short, long)]
    to: Option<u64>,
}

#[derive(ValueEnum, Clone, Copy, PartialEq, Eq)]
enum RecordKind {
    CommitRecord,
    CommitStatus,
    Statistics,
}

fn main() {
    let cli = Cli::parse();

    let source_wal = WAL::load(cli.source_directory).unwrap();

    let mut source_wal = WALClient::new(source_wal);
    source_wal.register_record_type::<Statistics>();
    source_wal.register_record_type::<CommitRecord>();
    source_wal.register_record_type::<StatusRecord>();

    match create_dir_all(cli.target_directory.join("wal")) {
        Ok(()) => (),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        err @ Err(_) => dbg!(err).unwrap(),
    }

    let mut target_wal = WALClient::new(WAL::load(cli.target_directory).unwrap());
    target_wal.register_record_type::<Statistics>();
    target_wal.register_record_type::<CommitRecord>();
    target_wal.register_record_type::<StatusRecord>();

    let from = DurabilitySequenceNumber::new(cli.from);
    let to = cli.to.map(DurabilitySequenceNumber::new).unwrap_or(source_wal.previous());
    for record in source_wal.iter_from(from).unwrap() {
        let record = record.unwrap();
        if record.sequence_number > to {
            break;
        }
        match record.record_type {
            CommitRecord::RECORD_TYPE if cli.kind.is_empty() || cli.kind.contains(&RecordKind::CommitRecord) => {
                _ = target_wal.sequenced_write::<CommitRecord>(&deserialise_record(&record.bytes)).unwrap()
            }
            StatusRecord::RECORD_TYPE if cli.kind.is_empty() || cli.kind.contains(&RecordKind::CommitStatus) => {
                target_wal.unsequenced_write::<StatusRecord>(&deserialise_record(&record.bytes)).unwrap()
            }
            Statistics::RECORD_TYPE if cli.kind.is_empty() || cli.kind.contains(&RecordKind::Statistics) => {
                target_wal.unsequenced_write::<Statistics>(&deserialise_record(&record.bytes)).unwrap()
            }
            CommitRecord::RECORD_TYPE | StatusRecord::RECORD_TYPE | Statistics::RECORD_TYPE => (), // filtered out
            unrecognized => panic!("Unrecognized record type: {unrecognized}"),
        }
    }
}

fn deserialise_record<Record: DurabilityRecord>(raw_bytes: &[u8]) -> Record {
    let ptr = &mut &*raw_bytes;
    Record::deserialise_from(ptr).unwrap()
}

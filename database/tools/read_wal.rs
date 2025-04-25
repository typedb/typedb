/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use concept::thing::statistics::Statistics;
use durability::{wal::WAL, DurabilitySequenceNumber, RawRecord};
use storage::{
    durability_client::{DurabilityClient, DurabilityRecord, WALClient},
    isolation_manager::{CommitRecord, StatusRecord},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The database directory location; the WAL is expected to be a subdirectory named 'wal' under this.
    #[arg(value_name = "DIR")]
    database_directory: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print wal data at a single sequence number
    Print {
        /// The sequence number of the records to print [default: last in the WAL]
        #[arg(value_name = "SEQ")]
        sequence_number: Option<u64>,
    },
    /// Print wal data in the range of sequence numbers
    PrintRange {
        /// The sequence number to start printing from (inclusive)
        #[arg(value_name = "FROM", default_value = "0")]
        sequence_number_from: u64,

        /// The sequence number to stop printing at (inclusive) [default: last in the WAL]
        #[arg(value_name = "TO")]
        sequence_number_to: Option<u64>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Print { sequence_number: None }) {
        Command::Print { sequence_number } => print_at(cli.database_directory, sequence_number),
        Command::PrintRange { sequence_number_from, sequence_number_to } => {
            print_range(cli.database_directory, sequence_number_from, sequence_number_to)
        }
    }
}

fn deserialise_record<Record: DurabilityRecord>(raw_bytes: &[u8]) -> Record {
    let ptr = &mut &*raw_bytes;
    Record::deserialise_from(ptr).unwrap()
}

fn print_record(record: RawRecord<'_>) {
    match record.record_type {
        CommitRecord::RECORD_TYPE => print_commit(record.sequence_number, deserialise_record(&record.bytes)),
        StatusRecord::RECORD_TYPE => print_status(record.sequence_number, deserialise_record(&record.bytes)),
        Statistics::RECORD_TYPE => print_statistics(record.sequence_number, deserialise_record(&record.bytes)),
        _ => print_raw_record(record),
    }
    println!();
}

fn print_commit(sequence_number: DurabilitySequenceNumber, commit: CommitRecord) {
    println!("commit data @ {}", sequence_number.number());
    println!("{:#?}", commit);
}

fn print_status(sequence_number: DurabilitySequenceNumber, status: StatusRecord) {
    println!("status @ {}", sequence_number.number());
    println!("{:?}", status);
}

fn print_statistics(sequence_number: DurabilitySequenceNumber, statistics: Statistics) {
    println!("statistics @ {}", sequence_number.number());
    println!("{:#?}", statistics);
}

fn print_raw_record(record: RawRecord<'_>) {
    const WIDTH: usize = 40;
    println!("Unrecognised record({}) @ {}", record.record_type, record.sequence_number.number());
    for (i, byte) in record.bytes.iter().enumerate() {
        print!("{byte:02X}");
        if (i + 1) % WIDTH == 0 {
            println!();
        } else if i % 2 == 1 {
            print!(" ");
        }
    }
    if record.bytes.len() % WIDTH != 0 {
        println!();
    }
}

fn print_range(path: PathBuf, from: u64, to: Option<u64>) {
    let wal = WAL::load(path).unwrap();

    let mut wal = WALClient::new(wal);
    wal.register_record_type::<Statistics>();
    wal.register_record_type::<CommitRecord>();
    wal.register_record_type::<StatusRecord>();

    let from = DurabilitySequenceNumber::new(from);
    let to = to.map(DurabilitySequenceNumber::new).unwrap_or(wal.previous());
    for record in wal.iter_from(from).unwrap() {
        let record = record.unwrap();
        if record.sequence_number > to {
            break;
        }
        print_record(record);
    }
}

fn print_at(path: PathBuf, sequence_number: Option<u64>) {
    let wal = WAL::load(path).unwrap();

    let mut wal = WALClient::new(wal);
    wal.register_record_type::<Statistics>();
    wal.register_record_type::<CommitRecord>();
    wal.register_record_type::<StatusRecord>();

    let sequence_number = sequence_number.map(DurabilitySequenceNumber::new).unwrap_or(wal.previous());
    for record in wal.iter_from(sequence_number).unwrap() {
        let record = record.unwrap();
        if record.sequence_number > sequence_number {
            break;
        }
        print_record(record);
    }
}

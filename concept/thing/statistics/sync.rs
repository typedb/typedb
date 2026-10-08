/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, sync::Arc};

use durability::RawRecord;
use itertools::Itertools;
use storage::{
    durability_client::{DurabilityClient, DurabilityRecord},
    record::{CommitRecord, LegacyCommitRecordV1, StatusRecord},
    sequence_number::SequenceNumber,
};

use crate::thing::statistics::{StatisticsError, deltas::CommitDeltas};

pub(crate) enum SyncRecord {
    Deltas(CommitDeltas),
    Commit(CommitRecord),
    Rejected,
}

pub(crate) fn load_commit_deltas(
    start: SequenceNumber,
    durability_client: &impl DurabilityClient,
) -> Result<BTreeMap<SequenceNumber, SyncRecord>, StatisticsError> {
    use StatisticsError::*;

    let mut commits = BTreeMap::new();
    for deltas in
        durability_client.iter_type_from::<CommitDeltas>(start).map_err(|typedb_source| Durability { typedb_source })?
    {
        let (_, deltas) = deltas.map_err(|typedb_source| Durability { typedb_source })?;
        let commit_sequence_number = deltas.commit_sequence_number;
        if commit_sequence_number < start {
            continue;
        }

        if commits.insert(commit_sequence_number, SyncRecord::Deltas(deltas)).is_some() {
            #[cfg(debug_assertions)]
            unreachable!("Encountered two sets of deltas for {commit_sequence_number:?}");
        }
    }

    if let Some(first_gap) = first_gap(&commits, start, durability_client.previous()) {
        let records = durability_client.iter_from(first_gap).map_err(|typedb_source| Durability { typedb_source })?;
        let mut pending = BTreeMap::new();
        for record in records {
            let RawRecord { sequence_number, record_type, bytes } =
                record.map_err(|typedb_source| Durability { typedb_source })?;
            match record_type {
                LegacyCommitRecordV1::RECORD_TYPE => {
                    if commits.contains_key(&sequence_number) {
                        continue;
                    }
                    let legacy = LegacyCommitRecordV1::deserialise_from(&mut &*bytes)
                        .map_err(|error| DurabilityRecordDeserialize { source: Arc::new(error) })?;
                    let commit_record = CommitRecord::from(legacy);
                    pending.insert(sequence_number, commit_record);
                }
                CommitRecord::RECORD_TYPE => {
                    if commits.contains_key(&sequence_number) {
                        continue;
                    }
                    let commit_record = CommitRecord::deserialise_from(&mut &*bytes)
                        .map_err(|error| DurabilityRecordDeserialize { source: Arc::new(error) })?;
                    pending.insert(sequence_number, commit_record);
                }
                StatusRecord::RECORD_TYPE => {
                    let status = StatusRecord::deserialise_from(&mut &*bytes)
                        .map_err(|error| DurabilityRecordDeserialize { source: Arc::new(error) })?;
                    if commits.contains_key(&status.commit_record_sequence_number()) {
                        continue;
                    }
                    let commit_sequence_number = status.commit_record_sequence_number();
                    let record = pending.remove(&commit_sequence_number);
                    if let Some(record) = record {
                        if status.was_committed() {
                            commits.insert(commit_sequence_number, SyncRecord::Commit(record));
                        } else {
                            commits.insert(commit_sequence_number, SyncRecord::Rejected);
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        unreachable!(
                            "Found a status record at {commit_sequence_number:?} without a corresponding commit record",
                        );
                    }
                }
                _ => (),
            }
        }
    }

    Ok(commits)
}

fn first_gap(
    map: &BTreeMap<SequenceNumber, SyncRecord>,
    start: SequenceNumber,
    end: SequenceNumber,
) -> Option<SequenceNumber> {
    let Some((&first, _)) = map.first_key_value() else { return Some(start) };
    if first > start {
        return Some(start);
    }

    if let Some(gap) = map.keys().tuple_windows().find_map(|(&prev, &next)| (prev.next() < next).then_some(prev)) {
        return Some(gap);
    }

    let Some((&last, _)) = map.last_key_value() else { return Some(start) };
    if last < end {
        return Some(last.next());
    }

    None
}

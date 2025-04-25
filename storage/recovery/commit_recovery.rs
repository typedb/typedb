/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, sync::Arc};

use durability::RawRecord;
use error::typedb_error;
use tracing::{event, Level};

use crate::{
    durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord},
    isolation_manager::{CommitRecord, IsolationManager, StatusRecord, ValidatedCommit},
    keyspace::{KeyspaceError, Keyspaces},
    sequence_number::SequenceNumber,
    write_batches::WriteBatches,
    MVCCStorage,
};

/// Load commit data from the start onwards. Ignores any statuses that are not paired with commit data.
pub fn load_commit_data_from(
    start: SequenceNumber,
    durability_client: &impl DurabilityClient,
    limit: usize,
) -> Result<BTreeMap<SequenceNumber, RecoveryCommitStatus>, StorageRecoveryError> {
    use StorageRecoveryError::{DurabilityClientRead, DurabilityRecordDeserialize, DurabilityRecordsMissing};

    let mut recovered_commits = BTreeMap::new();

    let records =
        durability_client.iter_from(start).map_err(|error| DurabilityClientRead { typedb_source: error })?.peekable();
    let mut first_record = true;

    for record in records {
        let RawRecord { sequence_number, record_type, bytes } =
            record.map_err(|error| DurabilityClientRead { typedb_source: error })?;
        if first_record {
            if sequence_number != start {
                return Err(DurabilityRecordsMissing {
                    expected_sequence_number: start,
                    first_record_sequence_number: sequence_number,
                });
            }
            first_record = false;
        }

        match record_type {
            CommitRecord::RECORD_TYPE => {
                let commit_record = CommitRecord::deserialise_from(&mut &*bytes)
                    .map_err(|error| DurabilityRecordDeserialize { source: Arc::new(error) })?;
                recovered_commits.insert(sequence_number, RecoveryCommitStatus::Pending(commit_record));
            }
            StatusRecord::RECORD_TYPE => {
                let StatusRecord { commit_record_sequence_number, was_committed } =
                    StatusRecord::deserialise_from(&mut &*bytes)
                        .map_err(|error| DurabilityRecordDeserialize { source: Arc::new(error) })?;
                if commit_record_sequence_number < start {
                    continue;
                }
                if was_committed {
                    let record = recovered_commits.remove(&commit_record_sequence_number).unwrap();
                    let RecoveryCommitStatus::Pending(record) = record else {
                        unreachable!("found second commit status for a record")
                    };
                    recovered_commits.insert(commit_record_sequence_number, RecoveryCommitStatus::Validated(record));
                } else {
                    recovered_commits.insert(commit_record_sequence_number, RecoveryCommitStatus::Rejected);
                }
            }
            _not_storage_record => {
                // skip, not storage record
            }
        }
        if recovered_commits.len() > limit {
            return Ok(recovered_commits);
        }
    }
    Ok(recovered_commits)
}

pub(crate) fn apply_recovered(
    recovered_commits: BTreeMap<SequenceNumber, RecoveryCommitStatus>,
    durability_client: &impl DurabilityClient,
    keyspaces: &Keyspaces,
) -> Result<(), StorageRecoveryError> {
    event!(Level::TRACE, "Applying recovered commits");
    use StorageRecoveryError::{DurabilityClientRead, DurabilityClientWrite, KeyspaceWrite};

    if recovered_commits.is_empty() {
        return Ok(());
    }

    let isolation_manager = IsolationManager::new(*recovered_commits.first_key_value().unwrap().0);

    let mut pending_writes = Vec::new();
    for (commit_sequence_number, commit) in recovered_commits {
        match commit {
            RecoveryCommitStatus::Validated(commit_record) => {
                pending_writes.push(WriteBatches::from_operations(commit_sequence_number, commit_record.operations()));
                isolation_manager.load_validated(commit_sequence_number, commit_record);
            }
            RecoveryCommitStatus::Rejected => isolation_manager.load_aborted(commit_sequence_number),
            RecoveryCommitStatus::Pending(commit_record) => {
                isolation_manager.opened_for_read(commit_record.open_sequence_number());
                let validated_commit = isolation_manager
                    .validate_commit(commit_sequence_number, commit_record, durability_client)
                    .map_err(|error| DurabilityClientRead { typedb_source: error })?;
                match validated_commit {
                    ValidatedCommit::Write(write_batches) => {
                        MVCCStorage::persist_commit_status(true, commit_sequence_number, durability_client)
                            .map_err(|error| DurabilityClientWrite { typedb_source: error })?;
                        pending_writes.push(write_batches);
                    }
                    ValidatedCommit::Conflict(_) => {
                        MVCCStorage::persist_commit_status(false, commit_sequence_number, durability_client)
                            .map_err(|error| DurabilityClientWrite { typedb_source: error })?;
                    }
                }
            }
        }
    }

    for write_batches in pending_writes {
        keyspaces.write(write_batches).map_err(|error| KeyspaceWrite { source: error })?;
    }

    Ok(())
}

pub enum RecoveryCommitStatus {
    Pending(CommitRecord),
    Validated(CommitRecord),
    Rejected,
}

typedb_error! {
    pub StorageRecoveryError(component = "Storage recovery", prefix = "REC") {
        DurabilityRecordDeserialize(1, "Failed to deserialise WAL record.", source: Arc<bincode::Error>),
        DurabilityClientRead(2, "Durability client read error.", typedb_source: DurabilityClientError),
        DurabilityClientWrite(3, "Durability client write error.", typedb_source: DurabilityClientError),
        DurabilityRecordsMissing(
            4,
            "Missing initial WAL records - expected first record number '{expected_sequence_number}', but found '{first_record_sequence_number}'.",
            expected_sequence_number: SequenceNumber, first_record_sequence_number: SequenceNumber
        ),
        KeyspaceWrite(5, "Error writing recovered commits to keyspace.", source: KeyspaceError),
    }
}

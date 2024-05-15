/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use durability::{RawRecord};
use crate::durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord};

use crate::isolation_manager::{CommitRecord, IsolationManager, StatusRecord, ValidatedCommit};
use crate::keyspace::{KeyspaceError, Keyspaces};
use crate::MVCCStorage;
use crate::recovery::commit_replay::CommitRecoveryError::DurabilityRecordsMissing;
use crate::sequence_number::SequenceNumber;
use crate::write_batches::WriteBatches;


/// Load commit data from the start onwards. Ignores any statuses that are not paired with commit data.
pub fn load_commit_data_from(
    start: SequenceNumber,
    durability_client: &impl DurabilityClient,
) -> Result<BTreeMap<SequenceNumber, RecoveryCommitStatus>, CommitRecoveryError> {
    use CommitRecoveryError::{DurabilityRecordDeserialize, DurabilityClientRead, DurabilityRecordsMissing};

    let mut recovered_commits = BTreeMap::new();

    let mut records = durability_client.iter_from(start).map_err(|error| DurabilityClientRead { source: error })?
        .peekable();
    let mut first_record = true;

    for record in records {
        let RawRecord { sequence_number, record_type, bytes } =
            record.map_err(|error| DurabilityClientRead { source: error })?;
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
                    .map_err(|error| DurabilityRecordDeserialize { source: error })?;
                recovered_commits.insert(sequence_number, RecoveryCommitStatus::Pending(commit_record));
            }
            StatusRecord::RECORD_TYPE => {
                let StatusRecord { commit_record_sequence_number, was_committed } = StatusRecord::deserialise_from(&mut &*bytes)
                    .map_err(|error| DurabilityRecordDeserialize { source: error })?;
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
            _other => unreachable!("Unexpected record read from durability service"),
        }
    }
    Ok(recovered_commits)
}

pub(crate) fn apply_commits(
    recovered_commits: BTreeMap<SequenceNumber, RecoveryCommitStatus>,
    durability_client: &impl DurabilityClient,
    keyspaces: &Keyspaces,
) -> Result<(), CommitRecoveryError> {
    use CommitRecoveryError::{DurabilityClientRead, DurabilityClientWrite, KeyspaceWrite};

    if recovered_commits.is_empty() {
        return Ok(());
    }

    let isolation_manager = IsolationManager::new(recovered_commits.first_key_value().unwrap().0.clone());

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
                    .map_err(|error| DurabilityClientRead { source: error })?;
                match validated_commit {
                    ValidatedCommit::Write(write_batches) => {
                        MVCCStorage::persist_commit_status(true, commit_sequence_number, durability_client)
                            .map_err(|error| DurabilityClientWrite { source: error })?;
                        pending_writes.push(write_batches);
                    }
                    ValidatedCommit::Conflict(_) => {
                        MVCCStorage::persist_commit_status(false, commit_sequence_number, durability_client)
                            .map_err(|error| DurabilityClientWrite { source: error })?;
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

#[derive(Debug)]
pub enum CommitRecoveryError {
    DurabilityRecordDeserialize { source: bincode::Error },
    DurabilityClientRead { source: DurabilityClientError },
    DurabilityClientWrite { source: DurabilityClientError },
    DurabilityRecordsMissing { expected_sequence_number: SequenceNumber, first_record_sequence_number: SequenceNumber },
    KeyspaceWrite { source: KeyspaceError },
}

impl fmt::Display for CommitRecoveryError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for CommitRecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DurabilityRecordDeserialize { source, .. } => Some(source),
            Self::DurabilityClientRead { source, .. } => Some(source),
            Self::DurabilityClientWrite { source, .. } => Some(source),
            Self::DurabilityRecordsMissing{ .. } => None,
            Self::KeyspaceWrite { source, .. } => Some(source),
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use chrono::Utc;
use durability::{DurabilityError, DurabilityRecord, DurabilityService, RawRecord, SequenceNumber};
use itertools::Itertools;

use crate::{
    isolation_manager::{CommitRecord, IsolationManager, StatusRecord},
    keyspace::{
        KeyspaceCheckpointError, KeyspaceError, KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError, Keyspaces,
    },
    write_batches::{self, WriteBatches},
    MVCCStorage, StorageCommitError, StorageOpenError,
};

pub(crate) struct Checkpoint {
    pub(crate) keyspaces: Keyspaces,
    pub(crate) next_sequence_number: SequenceNumber,
}

impl Checkpoint {
    const CHECKPOINT_DIR_NAME: &'static str = "checkpoint";
    const CHECKPOINT_METADATA_FILE_NAME: &'static str = "METADATA";

    pub(crate) fn create(
        watermark: SequenceNumber,
        storage_path: &Path,
        keyspaces: &Keyspaces,
    ) -> Result<PathBuf, CheckpointCreateError> {
        use CheckpointCreateError::{
            CheckpointDirCreate, CheckpointDirRead, KeyspaceCheckpoint, MetadataFileCreate, MetadataWrite,
            OldCheckpointRemove,
        };

        let checkpoint_dir = storage_path.join(Self::CHECKPOINT_DIR_NAME);
        if !checkpoint_dir.exists() {
            fs::create_dir_all(&checkpoint_dir)
                .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: error })?
        }

        let previous_checkpoints: Vec<_> = fs::read_dir(&checkpoint_dir)
            .and_then(|entries| entries.map_ok(|entry| entry.path()).try_collect())
            .map_err(|error| CheckpointDirRead { dir: checkpoint_dir.clone(), source: error })?;

        let current_checkpoint_dir = checkpoint_dir.join(format!("{}", Utc::now().timestamp_micros()));
        fs::create_dir_all(&current_checkpoint_dir)
            .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: error })?;

        keyspaces
            .checkpoint(&current_checkpoint_dir)
            .map_err(|error| KeyspaceCheckpoint { dir: current_checkpoint_dir.clone(), source: error })?;

        let metadata_file_path = current_checkpoint_dir.join(Self::CHECKPOINT_METADATA_FILE_NAME);
        let mut metadata_file = File::create(&metadata_file_path)
            .map_err(|error| MetadataFileCreate { file_path: metadata_file_path.clone(), source: error })?;
        metadata_file
            .write_all(watermark.number().to_string().as_bytes())
            .and_then(|()| metadata_file.sync_all())
            .map_err(|error| MetadataWrite { file_path: metadata_file_path.clone(), source: error })?;

        for previous_checkpoint in previous_checkpoints {
            fs::remove_dir_all(&previous_checkpoint)
                .map_err(|error| OldCheckpointRemove { dir: previous_checkpoint, source: error })?
        }

        Ok(current_checkpoint_dir)
    }

    pub(crate) fn load<KS: KeyspaceSet, Durability: DurabilityService>(
        storage_path: &Path,
        durability_service: &Durability,
    ) -> Result<Self, CheckpointLoadError> {
        use CheckpointLoadError::{KeyspaceWrite, MetadataRead};

        let checkpoint_dir = storage_path.join(Self::CHECKPOINT_DIR_NAME);
        let storage_dir = storage_path.join(MVCCStorage::<Durability>::STORAGE_DIR_NAME);

        let checkpoint_sequence_number = {
            if let Some(latest_checkpoint_dir) = find_latest_checkpoint(&checkpoint_dir)? {
                for keyspace in KS::iter() {
                    let keyspace_checkpoint_dir = latest_checkpoint_dir.join(keyspace.name());
                    let keyspace_dir = storage_dir.join(keyspace.name());
                    fs_utils::copy_dir_all(keyspace_checkpoint_dir, keyspace_dir).map_err(|_error| todo!())?;
                }
                let metadata_file_path = latest_checkpoint_dir.join(Self::CHECKPOINT_METADATA_FILE_NAME);
                let metadata = fs::read_to_string(metadata_file_path)
                    .map_err(|error| MetadataRead { dir: checkpoint_dir.clone(), source: error })?;
                SequenceNumber::new(metadata.parse().unwrap()) // FIXME corrupt METADATA handling
            } else {
                SequenceNumber::MIN
            }
        };

        let keyspaces = Keyspaces::open::<KS>(&storage_dir)?;

        let recovery_start = checkpoint_sequence_number + 1;
        let (pending_writes, next_sequence_number) = recover_isolation(recovery_start, durability_service)?;

        for write_batches in pending_writes {
            keyspaces.write(write_batches).map_err(|error| KeyspaceWrite { source: error })?;
            // Inform the isolation manager and increment the watermark
        }

        Ok(Self { keyspaces, next_sequence_number })
    }
}

fn recover_isolation(
    recovery_start: SequenceNumber,
    durability_service: &impl DurabilityService,
) -> Result<(Vec<WriteBatches>, SequenceNumber), CheckpointLoadError> {
    use CheckpointLoadError::*;

    let isolation_manager = IsolationManager::new(recovery_start);

    enum CheckpointCommitStatus {
        Pending(CommitRecord),
        Validated(Option<CommitRecord>),
        Rejected,
    }

    let mut recovered_commits = BTreeMap::new();
    for record in
        durability_service.iter_from(recovery_start).map_err(|error| DurabilityServiceRead { source: error })?
    {
        let RawRecord { sequence_number, record_type, bytes } =
            record.map_err(|error| DurabilityServiceRead { source: error })?;
        match record_type {
            CommitRecord::RECORD_TYPE => {
                let commit_record = CommitRecord::deserialise_from(&mut &*bytes).unwrap(); // FIXME
                recovered_commits.insert(sequence_number, CheckpointCommitStatus::Pending(commit_record));
            }
            StatusRecord::RECORD_TYPE => {
                let StatusRecord { commit_record_sequence_number, was_committed } =
                    StatusRecord::deserialise_from(&mut &*bytes).unwrap(); // FIXME
                if was_committed {
                    let record = recovered_commits.remove(&commit_record_sequence_number).map(|status| {
                        let CheckpointCommitStatus::Pending(record) = status else {
                            unreachable!("found second commit status for a record")
                        };
                        record
                    });
                    recovered_commits.insert(commit_record_sequence_number, CheckpointCommitStatus::Validated(record));
                } else {
                    recovered_commits.insert(commit_record_sequence_number, CheckpointCommitStatus::Rejected);
                }
            }
            _other => unreachable!("Unexpected record read from durability service"),
        }
    }

    let next_sequence_number = recovered_commits.keys().max().copied().unwrap_or(recovery_start - 1) + 1;

    let mut pending_writes = Vec::new();
    for (commit_sequence_number, commit) in recovered_commits {
        match commit {
            CheckpointCommitStatus::Validated(commit_record) => {
                let commit_record = commit_record.unwrap();
                pending_writes.push(WriteBatches::from_operations(commit_sequence_number, commit_record.operations()));
                isolation_manager.load_validated(commit_sequence_number, commit_record);
            }
            CheckpointCommitStatus::Rejected => isolation_manager.load_aborted(commit_sequence_number),
            CheckpointCommitStatus::Pending(commit_record) => {
                isolation_manager.opened_for_read(commit_record.open_sequence_number());
                let (conflict, write_batches) = isolation_manager
                    .validate_commit(commit_sequence_number, commit_record, durability_service)
                    .map_err(|error| DurabilityServiceRead { source: error })?;
                MVCCStorage::persist_commit_status(conflict.is_none(), commit_sequence_number, durability_service)
                    .map_err(|error| DurabilityServiceWrite { source: error })?;
                if conflict.is_none() {
                    pending_writes.push(write_batches.unwrap());
                }
            }
        }
    }

    Ok((pending_writes, next_sequence_number))
}

fn find_latest_checkpoint(checkpoint_dir: &Path) -> Result<Option<PathBuf>, CheckpointLoadError> {
    if !checkpoint_dir.exists() {
        return Ok(None);
    }

    fs::read_dir(checkpoint_dir)
        .and_then(|mut entries| entries.try_fold(None, |cur, entry| Ok(cur.max(Some(entry?.path())))))
        .map_err(|error| CheckpointLoadError::CheckpointRead { dir: checkpoint_dir.to_owned(), source: error })
}

#[derive(Debug)]
pub enum CheckpointCreateError {
    CheckpointDirCreate { dir: PathBuf, source: io::Error },
    CheckpointDirRead { dir: PathBuf, source: io::Error },

    KeyspaceCheckpoint { dir: PathBuf, source: KeyspaceCheckpointError },

    MetadataFileCreate { file_path: PathBuf, source: io::Error },
    MetadataWrite { file_path: PathBuf, source: io::Error },

    OldCheckpointRemove { dir: PathBuf, source: io::Error },
}

impl fmt::Display for CheckpointCreateError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for CheckpointCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointDirCreate { source, .. } => Some(source),
            Self::CheckpointDirRead { source, .. } => Some(source),
            Self::KeyspaceCheckpoint { source, .. } => Some(source),
            Self::MetadataFileCreate { source, .. } => Some(source),
            Self::MetadataWrite { source, .. } => Some(source),
            Self::OldCheckpointRemove { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum CheckpointLoadError {
    CheckpointRead { dir: PathBuf, source: io::Error },
    MetadataRead { dir: PathBuf, source: io::Error },
    Commit { source: StorageCommitError }, // TODO
    DurabilityServiceRead { source: DurabilityError },
    DurabilityServiceWrite { source: DurabilityError },
    KeyspaceValidation { source: KeyspaceValidationError },
    KeyspaceOpen { source: KeyspaceOpenError },
    KeyspaceWrite { source: KeyspaceError },
}

impl fmt::Display for CheckpointLoadError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for CheckpointLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointRead { source, .. } => Some(source),
            Self::MetadataRead { source, .. } => Some(source),
            Self::Commit { source, .. } => Some(source),
            Self::DurabilityServiceRead { source, .. } => Some(source),
            Self::DurabilityServiceWrite { source, .. } => Some(source),
            Self::KeyspaceValidation { source, .. } => Some(source),
            Self::KeyspaceOpen { source, .. } => Some(source),
            Self::KeyspaceWrite { source, .. } => Some(source),
        }
    }
}

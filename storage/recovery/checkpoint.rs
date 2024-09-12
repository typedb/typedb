/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::Utc;
use itertools::Itertools;
use same_file::is_same_file;

use crate::{
    durability_client::DurabilityClient,
    keyspace::{KeyspaceCheckpointError, KeyspaceOpenError, KeyspaceSet, Keyspaces},
    recovery::commit_replay::{apply_commits, load_commit_data_from, CommitRecoveryError},
    sequence_number::SequenceNumber,
    StorageCommitError,
};

/// A checkpoint is a directory, which contains at least the storage checkpointing data: keyspaces + the watermark.
/// The watermark represents a sequence number that is guaranteed to be in all the keyspaces, and after which we may
/// have to reapply commits to the keyspaces from the WAL.
pub struct Checkpoint {
    pub directory: PathBuf,
}

impl Checkpoint {
    const CHECKPOINT_DIR_NAME: &'static str = "checkpoint";
    const STORAGE_METADATA_FILE_NAME: &'static str = "STORAGE_METADATA";

    pub fn new(storage_path: &Path) -> Result<Self, CheckpointCreateError> {
        use CheckpointCreateError::CheckpointDirCreate;

        let checkpoint_dir = storage_path.join(Self::CHECKPOINT_DIR_NAME);
        if !checkpoint_dir.exists() {
            fs::create_dir_all(&checkpoint_dir)
                .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: Arc::new(error) })?
        }

        let current_checkpoint_dir = checkpoint_dir.join(format!("{}", Utc::now().timestamp_micros()));
        fs::create_dir_all(&current_checkpoint_dir)
            .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: Arc::new(error) })?;

        Ok(Checkpoint { directory: current_checkpoint_dir })
    }

    pub fn add_storage(&self, keyspaces: &Keyspaces, watermark: SequenceNumber) -> Result<(), CheckpointCreateError> {
        use CheckpointCreateError::{KeyspaceCheckpoint, MetadataFileCreate, MetadataWrite};
        keyspaces
            .checkpoint(&self.directory)
            .map_err(|error| KeyspaceCheckpoint { dir: self.directory.clone(), source: error })?;

        let metadata_file_path = self.directory.join(Self::STORAGE_METADATA_FILE_NAME);
        let mut metadata_file = File::create(&metadata_file_path)
            .map_err(|error| MetadataFileCreate { file_path: metadata_file_path.clone(), source: Arc::new(error) })?;
        metadata_file
            .write_all(watermark.number().to_string().as_bytes())
            .and_then(|()| metadata_file.sync_all())
            .map_err(|error| MetadataWrite { file_path: metadata_file_path.clone(), source: Arc::new(error) })?;
        Ok(())
    }

    pub fn add_extension<T: CheckpointExtension>(&self, data: &T) -> Result<(), CheckpointCreateError> {
        use CheckpointCreateError::{ExtensionDuplicate, ExtensionIO, ExtensionSerialise};
        let file_name = T::NAME;
        let path = self.directory.join(file_name);
        if path.exists() {
            return Err(ExtensionDuplicate { name: T::NAME.to_string() });
        }

        let mut file =
            File::create(path).map_err(|err| ExtensionIO { name: T::NAME.to_string(), source: Arc::new(err) })?;

        data.serialise_into(&mut file)
            .map_err(|err| ExtensionSerialise { name: T::NAME.to_string(), source: Arc::new(err) })?;

        Ok(())
    }

    pub fn finish(&self) -> Result<(), CheckpointCreateError> {
        use CheckpointCreateError::{CheckpointDirRead, MissingStorageData, OldCheckpointRemove};

        if !self.directory.join(Self::STORAGE_METADATA_FILE_NAME).exists() {
            return Err(MissingStorageData { dir: self.directory.clone() });
        }

        let previous_checkpoints: Vec<_> = fs::read_dir(self.directory.parent().unwrap())
            .and_then(|entries| {
                entries
                    .map_ok(|entry| entry.path())
                    .filter(|path| path.is_ok() && path.as_ref().unwrap() != &self.directory)
                    .try_collect()
            })
            .map_err(|error| CheckpointDirRead { dir: self.directory.clone(), source: Arc::new(error) })?;

        for previous_checkpoint in previous_checkpoints {
            fs::remove_dir_all(&previous_checkpoint)
                .map_err(|error| OldCheckpointRemove { dir: previous_checkpoint, source: Arc::new(error) })?
        }

        Ok(())
    }

    pub fn open_latest(storage_path: &Path) -> Result<Option<Self>, CheckpointLoadError> {
        let checkpoint_dir = storage_path.join(Self::CHECKPOINT_DIR_NAME);
        find_latest_checkpoint(&checkpoint_dir).map(|path| path.map(|p| Checkpoint { directory: p }))
    }

    pub fn get_extension<T: CheckpointExtension>(&self) -> Result<T, CheckpointLoadError> {
        use CheckpointLoadError::{ExtensionDeserialise, ExtensionIO, ExtensionNotFound};

        let file_name = T::NAME;
        let path = self.directory.join(file_name);
        if !path.exists() {
            return Err(ExtensionNotFound { name: T::NAME.to_string() });
        }

        let mut file =
            File::open(path).map_err(|err| ExtensionIO { name: T::NAME.to_string(), source: Arc::new(err) })?;

        let deserialised = T::deserialise_from(&mut file)
            .map_err(|err| ExtensionDeserialise { name: T::NAME.to_string(), source: Arc::new(err) })?;
        Ok(deserialised)
    }

    pub(crate) fn recover_storage<KS: KeyspaceSet, Durability: DurabilityClient>(
        &self,
        keyspaces_dir: &Path,
        durability_client: &Durability,
    ) -> Result<(Keyspaces, SequenceNumber), CheckpointLoadError> {
        use CheckpointLoadError::{CheckpointRestore, CommitRecoveryFailed, KeyspaceOpen};

        for keyspace in KS::iter() {
            let keyspace_dir = keyspaces_dir.join(keyspace.name());
            let keyspace_checkpoint_dir = self.directory.join(keyspace.name());
            restore_storage_from_checkpoint(keyspace_dir, keyspace_checkpoint_dir)
                .map_err(|error| CheckpointRestore { dir: self.directory.clone(), source: Arc::new(error) })?;
        }

        let keyspaces = Keyspaces::open::<KS>(&keyspaces_dir).map_err(|error| KeyspaceOpen { source: error })?;

        let recovery_start = self.read_sequence_number()? + 1;
        let recovered_commits = load_commit_data_from(recovery_start, durability_client)
            .map_err(|err| CommitRecoveryFailed { source: err })?;
        let next_sequence_number = recovered_commits.keys().max().copied().unwrap_or(recovery_start - 1) + 1;
        apply_commits(recovered_commits, durability_client, &keyspaces)
            .map_err(|err| CommitRecoveryFailed { source: err })?;
        Ok((keyspaces, next_sequence_number))
    }

    pub fn read_sequence_number(&self) -> Result<SequenceNumber, CheckpointLoadError> {
        use CheckpointLoadError::MetadataRead;

        let metadata_file_path = self.directory.join(Self::STORAGE_METADATA_FILE_NAME);
        let metadata = fs::read_to_string(metadata_file_path)
            .map_err(|error| MetadataRead { dir: self.directory.clone(), source: Arc::new(error) })?;
        Ok(SequenceNumber::new(
            metadata.parse().expect("Could not read METADATA file (could try to restore from previous checkpoint)"),
        ))
    }
}

fn restore_storage_from_checkpoint(keyspace_dir: PathBuf, keyspace_checkpoint_dir: PathBuf) -> io::Result<()> {
    fs::create_dir_all(&keyspace_dir)?;

    for entry in fs::read_dir(&keyspace_dir)? {
        let storage_file = entry?.path();
        let checkpoint_file = keyspace_checkpoint_dir.join(storage_file.file_name().unwrap());
        if !checkpoint_file.exists() {
            fs::remove_file(storage_file)?;
        }
    }

    for entry in fs::read_dir(&keyspace_checkpoint_dir)? {
        let checkpoint_file = entry?.path();
        let storage_file = keyspace_dir.join(checkpoint_file.file_name().unwrap());
        if !storage_file.exists() || !is_same_file(&storage_file, &checkpoint_file)? {
            fs::copy(checkpoint_file, storage_file)?;
        }
    }

    Ok(())
}

fn find_latest_checkpoint(checkpoint_dir: &Path) -> Result<Option<PathBuf>, CheckpointLoadError> {
    if !checkpoint_dir.exists() {
        return Ok(None);
    }

    fs::read_dir(checkpoint_dir)
        .and_then(|mut entries| entries.try_fold(None, |cur, entry| Ok(cur.max(Some(entry?.path())))))
        .map_err(|error| CheckpointLoadError::CheckpointRead {
            dir: checkpoint_dir.to_owned(),
            source: Arc::new(error),
        })
}

pub trait CheckpointExtension: Sized {
    const NAME: &'static str;
    fn serialise_into(&self, writer: &mut impl Write) -> bincode::Result<()>;
    fn deserialise_from(reader: &mut impl Read) -> bincode::Result<Self>;
}

#[derive(Debug, Clone)]
pub enum CheckpointCreateError {
    CheckpointDirCreate { dir: PathBuf, source: Arc<io::Error> },
    CheckpointDirRead { dir: PathBuf, source: Arc<io::Error> },

    MissingStorageData { dir: PathBuf },

    KeyspaceCheckpoint { dir: PathBuf, source: KeyspaceCheckpointError },

    MetadataFileCreate { file_path: PathBuf, source: Arc<io::Error> },
    MetadataWrite { file_path: PathBuf, source: Arc<io::Error> },

    ExtensionDuplicate { name: String },
    ExtensionIO { name: String, source: Arc<io::Error> },
    ExtensionSerialise { name: String, source: Arc<bincode::Error> },

    OldCheckpointRemove { dir: PathBuf, source: Arc<io::Error> },
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
            Self::MissingStorageData { .. } => None,
            Self::KeyspaceCheckpoint { source, .. } => Some(source),
            Self::MetadataFileCreate { source, .. } => Some(source),
            Self::MetadataWrite { source, .. } => Some(source),
            Self::ExtensionDuplicate { .. } => None,
            Self::ExtensionIO { source, .. } => Some(source),
            Self::ExtensionSerialise { source, .. } => Some(source),
            Self::OldCheckpointRemove { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CheckpointLoadError {
    CheckpointRead { dir: PathBuf, source: Arc<io::Error> },
    MetadataRead { dir: PathBuf, source: Arc<io::Error> },
    CheckpointNotFound { dir: PathBuf },
    CommitRecoveryFailed { source: CommitRecoveryError },
    CheckpointRestore { dir: PathBuf, source: Arc<io::Error> },
    CommitPending { source: StorageCommitError },
    KeyspaceOpen { source: KeyspaceOpenError },

    ExtensionNotFound { name: String },
    ExtensionIO { name: String, source: Arc<io::Error> },
    ExtensionDeserialise { name: String, source: Arc<bincode::Error> },
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
            Self::CheckpointNotFound { .. } => None,
            Self::CommitRecoveryFailed { source, .. } => Some(source),
            Self::CheckpointRestore { source, .. } => Some(source),
            Self::MetadataRead { source, .. } => Some(source),
            Self::CommitPending { source, .. } => Some(source),
            Self::KeyspaceOpen { source, .. } => Some(source),

            Self::ExtensionNotFound { .. } => None,
            Self::ExtensionIO { source, .. } => Some(source),
            Self::ExtensionDeserialise { source, .. } => Some(source),
        }
    }
}

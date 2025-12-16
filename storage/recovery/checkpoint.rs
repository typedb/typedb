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
use error::typedb_error;
use itertools::Itertools;
use same_file::is_same_file;
use tracing::trace;

use crate::{
    durability_client::DurabilityClient,
    keyspace::{KeyspaceCheckpointError, KeyspaceOpenError, KeyspaceSet, Keyspaces},
    recovery::commit_recovery::{apply_recovered, load_commit_data_from, StorageRecoveryError},
    sequence_number::SequenceNumber,
};

const CHECKPOINT_DIR_NAME: &str = "checkpoint";
const STORAGE_METADATA_FILE_NAME: &str = "STORAGE_METADATA";
const TEMP_FILE_EXTENSION: &str = "tmp";

/// A checkpoint is a directory, which contains at least the storage checkpointing data: keyspaces + the watermark.
/// The watermark represents a sequence number that is guaranteed to be in all the keyspaces, and after which we may
/// have to reapply commits to the keyspaces from the WAL.
pub struct Checkpoint {
    pub directory: PathBuf,
}

impl Checkpoint {
    pub fn open_latest<KS: KeyspaceSet>(storage_path: &Path) -> Result<Option<Self>, CheckpointLoadError> {
        use CheckpointLoadError::CheckpointRead;

        let checkpoint_dir = storage_path.join(CHECKPOINT_DIR_NAME);
        if !checkpoint_dir.exists() {
            return Ok(None);
        }

        fs::read_dir(&checkpoint_dir)
            .and_then(|mut entries| {
                entries.try_fold(None, |cur, entry| {
                    let path = entry?.path();
                    if path.extension() == Some(TEMP_FILE_EXTENSION.as_ref()) {
                        // skip unfinished checkpoint
                        return Ok(cur);
                    }

                    if cur.as_ref().is_some_and(|cur: &Checkpoint| cur.directory > path) {
                        return Ok(cur);
                    }

                    let checkpoint = Checkpoint { directory: path };
                    if checkpoint.is_consistent::<KS>()? {
                        Ok(Some(checkpoint))
                    } else {
                        Ok(cur)
                    }
                })
            })
            .map_err(|error| CheckpointRead { dir: checkpoint_dir, source: Arc::new(error) })
    }

    pub fn get_additional_data<T: CheckpointAdditionalData>(&self) -> Result<T, CheckpointLoadError> {
        use CheckpointLoadError::{AdditionalDataDeserialise, AdditionalDataIO, AdditionalDataNotFound};

        let file_name = T::NAME;
        let path = self.directory.join(file_name);
        if !path.exists() {
            return Err(AdditionalDataNotFound { name: T::NAME.to_string() });
        }

        let mut file =
            File::open(path).map_err(|err| AdditionalDataIO { name: T::NAME.to_string(), source: Arc::new(err) })?;

        let deserialised = T::deserialise_from(&mut file)
            .map_err(|err| AdditionalDataDeserialise { name: T::NAME.to_string(), source: Arc::new(err) })?;
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
            trace!("Recovering keyspace from checkpoint");
            restore_storage_from_checkpoint(keyspace_dir, keyspace_checkpoint_dir)
                .map_err(|error| CheckpointRestore { dir: self.directory.clone(), source: Arc::new(error) })?;
        }

        let keyspaces = Keyspaces::open::<KS>(&keyspaces_dir).map_err(|error| KeyspaceOpen { source: error })?;

        trace!("Finished recovering keyspaces, recovering missing commits");

        let checkpoint_sequence_number = self.read_sequence_number()?;
        if checkpoint_sequence_number > durability_client.previous() {
            panic!("The checkpoint is ahead of the durability service! The durability logs may have been corrupted. Aborting.");
        }

        let recovery_start = checkpoint_sequence_number + 1;
        let recovered_commits = load_commit_data_from(recovery_start, durability_client, usize::MAX)
            .map_err(|err| CommitRecoveryFailed { typedb_source: err })?;
        let next_sequence_number = recovered_commits.keys().max().copied().unwrap_or(recovery_start - 1) + 1;
        trace!("Applying missing commits");
        apply_recovered(recovered_commits, durability_client, &keyspaces)
            .map_err(|err| CommitRecoveryFailed { typedb_source: err })?;
        Ok((keyspaces, next_sequence_number))
    }

    pub fn read_sequence_number(&self) -> Result<SequenceNumber, CheckpointLoadError> {
        use CheckpointLoadError::MetadataRead;

        let metadata_file_path = self.directory.join(STORAGE_METADATA_FILE_NAME);
        let metadata = fs::read_to_string(metadata_file_path)
            .map_err(|error| MetadataRead { dir: self.directory.clone(), source: Arc::new(error) })?;
        Ok(SequenceNumber::new(
            metadata.parse().expect("Could not read METADATA file (could try to restore from previous checkpoint)"),
        ))
    }

    fn is_consistent<KS: KeyspaceSet>(&self) -> io::Result<bool> {
        if !self.directory.is_dir() {
            return Ok(false);
        }
        if !self.directory.join(STORAGE_METADATA_FILE_NAME).exists() {
            return Ok(false);
        }
        for keyspace in KS::iter() {
            let keyspace_checkpoint_dir = self.directory.join(keyspace.name());
            if !fs::exists(keyspace_checkpoint_dir)? {
                return Ok(false);
            }
        }
        let metadata_file_path = self.directory.join(STORAGE_METADATA_FILE_NAME);
        fs::exists(metadata_file_path)
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
            copy_file(&checkpoint_file, &storage_file)?;
        }
    }

    Ok(())
}

/// A checkpoint is a directory, which contains at least the storage checkpointing data: keyspaces + the watermark.
/// The watermark represents a sequence number that is guaranteed to be in all the keyspaces, and after which we may
/// have to reapply commits to the keyspaces from the WAL.
pub struct CheckpointWriter {
    pub checkpoint_directory: PathBuf,
    pub temporary_directory: PathBuf,
}

impl CheckpointWriter {
    pub fn new(storage_path: &Path) -> Result<Self, CheckpointCreateError> {
        use CheckpointCreateError::CheckpointDirCreate;

        let checkpoint_dir = storage_path.join(CHECKPOINT_DIR_NAME);
        if !checkpoint_dir.exists() {
            fs::create_dir_all(&checkpoint_dir)
                .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: Arc::new(error) })?
        }

        let checkpoint_directory = checkpoint_dir.join(format!("{}", Utc::now().timestamp_micros(),));
        let temporary_directory = checkpoint_directory.with_extension(TEMP_FILE_EXTENSION);
        fs::create_dir_all(&temporary_directory)
            .map_err(|error| CheckpointDirCreate { dir: checkpoint_dir.clone(), source: Arc::new(error) })?;

        Ok(Self { checkpoint_directory, temporary_directory })
    }

    pub fn add_storage(&self, keyspaces: &Keyspaces, watermark: SequenceNumber) -> Result<(), CheckpointCreateError> {
        use CheckpointCreateError::{KeyspaceCheckpoint, MetadataWrite};

        keyspaces
            .checkpoint(&self.temporary_directory)
            .map_err(|error| KeyspaceCheckpoint { dir: self.temporary_directory.clone(), source: error })?;

        let metadata_file_path = self.temporary_directory.join(STORAGE_METADATA_FILE_NAME);
        write_file(&metadata_file_path, watermark.number().to_string().as_bytes())
            .map_err(|e| MetadataWrite { file_path: metadata_file_path, source: Arc::new(e) })?;

        Ok(())
    }

    pub fn add_extension<T: CheckpointAdditionalData>(&self, data: &T) -> Result<(), CheckpointCreateError> {
        use CheckpointCreateError::{ExtensionDuplicate, ExtensionIO, ExtensionSerialise};
        let file_name = T::NAME;
        let path = self.temporary_directory.join(file_name);
        if path.exists() {
            return Err(ExtensionDuplicate { name: T::NAME.to_string() });
        }

        let tmp = path.with_extension(TEMP_FILE_EXTENSION);
        {
            let mut file =
                File::create(&tmp).map_err(|err| ExtensionIO { name: T::NAME.to_string(), source: Arc::new(err) })?;
            data.serialise_into(&mut file)
                .map_err(|err| ExtensionSerialise { name: T::NAME.to_string(), source: Arc::new(err) })?;
        }
        fs::rename(&tmp, &path).map_err(|err| ExtensionIO { name: T::NAME.to_string(), source: Arc::new(err) })?;

        Ok(())
    }

    pub fn finish(self) -> Result<Checkpoint, CheckpointCreateError> {
        use CheckpointCreateError::{CheckpointDirCreate, CheckpointDirRead, MissingStorageData, OldCheckpointRemove};

        if !self.temporary_directory.join(STORAGE_METADATA_FILE_NAME).exists() {
            return Err(MissingStorageData { dir: self.temporary_directory.clone() });
        }

        let previous_checkpoints: Vec<_> = fs::read_dir(self.temporary_directory.parent().unwrap())
            .and_then(|entries| {
                entries
                    .map_ok(|entry| entry.path())
                    .filter(|path| path.is_ok() && path.as_ref().unwrap() != &self.temporary_directory)
                    .try_collect()
            })
            .map_err(|error| CheckpointDirRead { dir: self.temporary_directory.clone(), source: Arc::new(error) })?;

        for previous_checkpoint in previous_checkpoints {
            fs::remove_dir_all(&previous_checkpoint)
                .map_err(|error| OldCheckpointRemove { dir: previous_checkpoint, source: Arc::new(error) })?
        }

        fs::rename(self.temporary_directory, &self.checkpoint_directory)
            .map_err(|error| CheckpointDirCreate { dir: self.checkpoint_directory.clone(), source: Arc::new(error) })?;

        Ok(Checkpoint { directory: self.checkpoint_directory })
    }
}

fn write_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> io::Result<()> {
    let mut source_file = File::open(source)?;
    let mut destination_file = File::create(destination)?;
    io::copy(&mut source_file, &mut destination_file)?;
    destination_file.sync_all()?;
    Ok(())
}

pub trait CheckpointAdditionalData: Sized {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
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

typedb_error! {
    pub CheckpointLoadError(component = "Checkpoint load.", prefix = "CLO") {
        CheckpointRead(1, "Error to reading checkpoint directory '{dir:?}'.", dir: PathBuf, source: Arc<io::Error>),
        MetadataRead(2, "Error reading checkpoint metadata file in directory '{dir:?}.", dir: PathBuf, source: Arc<io::Error>),
        CheckpointNotFound(3, "No checkpoints found in directory '{dir:?}.", dir: PathBuf),
        CommitRecoveryFailed(4, "Failed to recover commits that are in the WAL but not in the storage layer.", typedb_source: StorageRecoveryError),
        CheckpointRestore(5, "Error restoring checkpoint in directory '{dir:?}'.)", dir: PathBuf, source: Arc<io::Error>),
        KeyspaceOpen(7, "Error while opening storage keyspaces.", source: KeyspaceOpenError),

        AdditionalDataNotFound(8, "Checkpoint additional data with identifier '{name}' not found.", name: String),
        AdditionalDataIO(9, "Error accessing checkpoint additional data with identifier '{name}'.", name: String, source: Arc<io::Error>),
        AdditionalDataDeserialise(10, "Error deserialising checkpoint additional data with identifier '{name}'.", name: String, source: Arc<bincode::Error>),
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use durability::properties::WALProperties;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use storage::properties::StorageProperties;
use versioning::{ExtendableVersion, IncompatibleVersion, ServerSupportedVersion};

use crate::database::DatabaseOpenError;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct DatabaseUid(u64);

impl DatabaseUid {
    fn next_random() -> Self {
        // Keep within i64 range so we can store in a TypeDB integer.
        Self(rand::thread_rng().next_u64() >> 1)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DatabaseProperties {
    properties_file_version: ExtendableVersion,
    database_id: DatabaseUid,
    wal: WALProperties,
    storage: StorageProperties,
}

impl DatabaseProperties {
    const FILENAME: &'static str = "properties.yml";
    const SERVER_PROPERTIES_FILE_VERSION: ServerSupportedVersion =
        ServerSupportedVersion(ExtendableVersion { version: 1, extension: 1 });

    pub(crate) fn values_for_pre_properties_database() -> Self {
        let database_id = DatabaseUid::next_random();
        let properties_file_version = Self::SERVER_PROPERTIES_FILE_VERSION.0.clone();
        let wal = WALProperties::values_for_pre_properties_database();
        let storage = StorageProperties::values_for_pre_properties_database();
        Self { database_id, properties_file_version, wal, storage }
    }

    pub(crate) fn create_for_new_database() -> Self {
        let database_id = DatabaseUid::next_random();
        let properties_file_version = Self::SERVER_PROPERTIES_FILE_VERSION.0.clone();
        let wal = WALProperties::create_for_new_database();
        let storage = StorageProperties::create_for_new_database();
        Self { database_id, properties_file_version, wal, storage }
    }

    pub(crate) fn validate_compatibility(properties: &Self) -> Result<(), DatabaseOpenError> {
        Self::SERVER_PROPERTIES_FILE_VERSION.check_compatible(&properties.properties_file_version).map_err(
            |mismatch| match mismatch {
                IncompatibleVersion::ResourceIsOlderThanServer => DatabaseOpenError::PropertiesFileTooOldForServer {
                    server_version: Self::SERVER_PROPERTIES_FILE_VERSION.0.clone(),
                    file_version: properties.properties_file_version.clone(),
                },
                IncompatibleVersion::ResourceIsNewerThanServer => DatabaseOpenError::PropertiesFileTooNewForServer {
                    server_version: Self::SERVER_PROPERTIES_FILE_VERSION.0.clone(),
                    file_version: properties.properties_file_version.clone(),
                },
            },
        )?;
        WALProperties::validate_compatibility(&properties.wal)
            .map_err(|source| DatabaseOpenError::WALOpen { source })?;
        StorageProperties::validate_compatibility(&properties.storage)
            .map_err(|typedb_source| DatabaseOpenError::StorageOpen { typedb_source })?;
        Ok(())
    }

    pub(crate) fn try_read(dir: &Path, db_name: &str) -> Result<Option<Self>, Box<DatabaseOpenError>> {
        let file_path = dir.join(Self::FILENAME);
        let exists = std::fs::exists(&file_path).map_err(|io_error| {
            Box::new(DatabaseOpenError::PropertiesRead { name: db_name.to_owned(), source: Arc::new(io_error) })
        })?;
        if !exists {
            return Ok(None);
        }
        let mut raw_yaml = String::new();
        File::open(&file_path).and_then(|mut file| file.read_to_string(&mut raw_yaml)).map_err(|io_error| {
            Box::new(DatabaseOpenError::PropertiesRead { name: db_name.to_owned(), source: Arc::new(io_error) })
        })?;
        let properties = serde_yaml2::from_str::<Self>(raw_yaml.as_str()).map_err(|source| {
            Box::new(DatabaseOpenError::PropertiesParse { name: db_name.to_owned(), source: Arc::new(source) })
        })?;
        Ok(Some(properties))
    }

    pub(crate) fn write(&self, dir: &Path, db_name: &str) -> Result<(), DatabaseOpenError> {
        let yaml = serde_yaml2::to_string(self).map_err(|serde_error| DatabaseOpenError::PropertiesSerialize {
            name: db_name.to_owned(),
            source: Arc::new(serde_error),
        })?;
        let write_error =
            |io_error| DatabaseOpenError::PropertiesWrite { name: db_name.to_owned(), source: Arc::new(io_error) };

        // Write to a temp file, fsync it, then rename over the real file and fsync the directory,
        // so a crash leaves either the old file or the new one, never a half-written one.
        let final_path = dir.join(Self::FILENAME);
        let tmp_path = dir.join(format!("{}.tmp", Self::FILENAME));

        let mut tmp_file = File::create(&tmp_path).map_err(write_error)?;
        tmp_file.write_all(yaml.as_bytes()).map_err(write_error)?;
        tmp_file.sync_all().map_err(write_error)?;
        drop(tmp_file);

        std::fs::rename(&tmp_path, &final_path).map_err(write_error)?;
        durability::sync_directory(dir).map_err(write_error)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use rand::RngCore;

    use crate::properties::DatabaseProperties;

    #[test]
    fn write_and_read() {
        fn write_and_readback(props: DatabaseProperties) {
            let dir =
                std::env::temp_dir().join(Path::new(&format!("dbprops_readback_{}", rand::thread_rng().next_u64())));
            if fs::exists(&dir).unwrap() {
                fs::remove_dir_all(&dir).unwrap();
            };
            fs::create_dir_all(&dir).unwrap();
            props.write(&dir, "testdb").unwrap();
            let readback = DatabaseProperties::try_read(&dir, "testdb").unwrap().unwrap();
            assert_eq!(readback, props);
            fs::remove_dir_all(&dir).unwrap();
        }

        write_and_readback(DatabaseProperties::create_for_new_database());

        write_and_readback(DatabaseProperties::values_for_pre_properties_database());
    }
}

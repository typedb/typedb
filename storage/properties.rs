/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::storage::{STORAGE_VERSION_VERSION, STORAGE_VERSION_EXTENSION};
use serde::{Deserialize, Serialize};
use versioning::{ExtendableVersion, IncompatibleVersion, ServerSupportedVersion};

use crate::StorageOpenError;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageProperties {
    pub version: ExtendableVersion,
}

impl StorageProperties {
    pub const SERVER_STORAGE_VERSION: ServerSupportedVersion = ServerSupportedVersion(ExtendableVersion {
        version: STORAGE_VERSION_VERSION,
        extension: STORAGE_VERSION_EXTENSION,
    });

    pub fn values_for_pre_properties_database() -> Self {
        Self { version: ExtendableVersion { version: 1, extension: 1 } }
    }

    pub fn create_for_new_database() -> Self {
        Self { version: Self::SERVER_STORAGE_VERSION.0.clone() }
    }

    pub fn validate_compatibility(database: &Self) -> Result<(), StorageOpenError> {
        Self::SERVER_STORAGE_VERSION.check_compatible(&database.version).map_err(
            |incompatibility| match incompatibility {
                IncompatibleVersion::ResourceIsOlderThanServer => StorageOpenError::StorageVersionTooOldForServer {
                    server_version: Self::SERVER_STORAGE_VERSION.0.clone(),
                    database_version: database.version.clone(),
                },
                IncompatibleVersion::ResourceIsNewerThanServer => StorageOpenError::StorageVersionTooNewForServer {
                    server_version: Self::SERVER_STORAGE_VERSION.0.clone(),
                    database_version: database.version.clone(),
                },
            },
        )?;
        Ok(())
    }
}

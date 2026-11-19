/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use resource::constants::storage::{WAL_VERSION_EXTENSION, WAL_VERSION_VERSION};
use serde::{Deserialize, Serialize};
use versioning::{ExtendableVersion, IncompatibleVersion, ServerSupportedVersion};

use crate::{DurabilityServiceError, wal::WALError};

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct WALProperties {
    pub version: ExtendableVersion,
}

impl WALProperties {
    pub const SERVER_WAL_VERSION: ServerSupportedVersion =
        ServerSupportedVersion(ExtendableVersion { version: WAL_VERSION_VERSION, extension: WAL_VERSION_EXTENSION });

    pub fn values_for_pre_properties_database() -> Self {
        Self { version: ExtendableVersion { version: 1, extension: 1 } }
    }

    pub fn create_for_new_database() -> Self {
        Self { version: Self::SERVER_WAL_VERSION.0.clone() }
    }

    pub fn validate_compatibility(database: &Self) -> Result<(), DurabilityServiceError> {
        Self::SERVER_WAL_VERSION.check_compatible(&database.version).map_err(
            |incompatibility| match incompatibility {
                IncompatibleVersion::ResourceIsOlderThanServer => DurabilityServiceError::WAL {
                    source: WALError::WALVersionTooOldForServer {
                        server_version: Self::SERVER_WAL_VERSION.0.clone(),
                        database_version: database.version.clone(),
                    },
                },
                IncompatibleVersion::ResourceIsNewerThanServer => DurabilityServiceError::WAL {
                    source: WALError::WALVersionTooNewForServer {
                        server_version: Self::SERVER_WAL_VERSION.0.clone(),
                        database_version: database.version.clone(),
                    },
                },
            },
        )?;
        Ok(())
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, fmt::Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendableVersion {
    pub version: u16,
    pub extension: u16,
}

impl fmt::Display for ExtendableVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.version, self.extension)
    }
}

#[derive(Debug)]
pub struct ServerSupportedVersion(pub ExtendableVersion);

impl ServerSupportedVersion {
    pub fn is_compatible(&self, resource: &ExtendableVersion) -> Result<(), IncompatibleVersion> {
        if self.0.version == resource.version {
            if self.0.extension >= resource.extension {
                Ok(())
            } else {
                Err(IncompatibleVersion::ResourceIsNewerThanServer)
            }
        } else {
            if self.0.version < resource.version {
                Err(IncompatibleVersion::ResourceIsOlderThanServer)
            } else {
                Err(IncompatibleVersion::ResourceIsNewerThanServer)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IncompatibleVersion {
    ResourceIsOlderThanServer,
    ResourceIsNewerThanServer,
}

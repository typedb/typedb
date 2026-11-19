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
            if self.0.version > resource.version {
                Err(IncompatibleVersion::ResourceIsOlderThanServer)
            } else {
                Err(IncompatibleVersion::ResourceIsNewerThanServer)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncompatibleVersion {
    ResourceIsOlderThanServer,
    ResourceIsNewerThanServer,
}

pub mod tests {
    use crate::{ExtendableVersion, IncompatibleVersion, ServerSupportedVersion};

    #[test]
    fn test_comaptibility_check() {
        fn tests(
            server_version: ServerSupportedVersion,
            cases: &[(&ExtendableVersion, Result<(), IncompatibleVersion>)],
        ) {
            for (resource_version, expected) in cases {
                let actual = server_version.is_compatible(resource_version);
                match (actual, expected) {
                    (Ok(()), Ok(())) => (),
                    (Err(a), Err(e)) => {
                        assert_eq!(a, *e, "Compatibility test returned the wrong error")
                    }
                    (Ok(()), Err(_)) | (Err(_), Ok(())) => {
                        panic!("Compatibility test is wrong. Expected: {expected:?}; Actual: {actual:?}");
                    }
                }
            }
        }

        let v1_1 = ExtendableVersion { version: 1, extension: 1 };
        let v1_2 = ExtendableVersion { version: 1, extension: 2 };
        let v2_1 = ExtendableVersion { version: 2, extension: 1 };

        tests(
            ServerSupportedVersion(v1_1.clone()),
            &[
                (&v1_1, Ok(())),
                (&v1_2, Err(IncompatibleVersion::ResourceIsNewerThanServer)),
                (&v2_1, Err(IncompatibleVersion::ResourceIsNewerThanServer)),
            ],
        );

        tests(
            ServerSupportedVersion(v1_2.clone()),
            &[(&v1_1, Ok(())), (&v1_2, Ok(())), (&v2_1, Err(IncompatibleVersion::ResourceIsNewerThanServer))],
        );
        tests(
            ServerSupportedVersion(v2_1.clone()),
            &[
                (&v1_1, Err(IncompatibleVersion::ResourceIsOlderThanServer)),
                (&v1_2, Err(IncompatibleVersion::ResourceIsOlderThanServer)),
                (&v2_1, Ok(())),
            ],
        );
    }
}

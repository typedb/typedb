/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, fmt::Formatter, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendableVersion {
    pub version: u16,
    pub extension: u16,
}

impl fmt::Display for ExtendableVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.version, self.extension)
    }
}

impl FromStr for ExtendableVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (version_str, extension_str) =
            s.split_once('.').ok_or_else(|| format!("expected '<version>.<extension>', got '{s}'"))?;
        let version = version_str.parse::<u16>().map_err(|_| format!("Invalid version: {version_str}"))?;
        let extension = extension_str.parse::<u16>().map_err(|_| format!("Invalid extension: {extension_str}"))?;
        Ok(Self { version, extension })
    }
}

impl Serialize for ExtendableVersion {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ExtendableVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = ExtendableVersion;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string of the form '<version>.<extension>'")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_string(Visitor)
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

#[cfg(test)]
pub mod tests {
    use crate::{ExtendableVersion, IncompatibleVersion, ServerSupportedVersion};

    #[test]
    fn test_compatibility_check() {
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

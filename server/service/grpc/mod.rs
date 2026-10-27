/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;

pub use error::{IntoGrpcStatus, IntoProtocolErrorMessage};

pub mod analyze;
pub(crate) mod authenticator;
pub(crate) mod concept;
mod diagnostics;
mod document;
pub(crate) mod encryption;
mod error;
pub(crate) mod migration;
mod options;
mod request_parser;
mod response_builders;
mod row;
pub(crate) mod transaction_service;
pub(crate) mod typedb_service;
pub(crate) type ConnectionID = uuid::Bytes;

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct GrpcProtocolVersion {
    version: i32,
    extension: i32,
}

impl GrpcProtocolVersion {
    pub(crate) fn new(version: i32, extension: i32) -> GrpcProtocolVersion {
        Self { version, extension }
    }
}

impl fmt::Display for GrpcProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for GrpcProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.version as i32, self.extension as i32)
    }
}

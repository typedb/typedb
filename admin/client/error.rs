/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

#[derive(Debug)]
pub enum AdminError {
    ConnectionFailed { address: String, source: tonic::transport::Error },
    RpcFailed { source: tonic::Status },
    InvalidArgCount { usage: &'static str },
    InvalidArgument { name: &'static str, reason: String },
    UnknownCommand { input: String },
    ScriptReadFailed { path: String, source: std::io::Error },
    Other { message: String },
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionFailed { address, source } => write!(f, "Failed to connect to {address}: {source}"),
            Self::RpcFailed { source } => {
                let code = source.code();
                let message = source.message();
                if message.is_empty() {
                    write!(f, "{code}")
                } else {
                    write!(f, "{code}: {message}")
                }
            }
            Self::InvalidArgCount { usage } => write!(f, "Usage: {usage}"),
            Self::InvalidArgument { name, reason } => write!(f, "Invalid argument '{name}': {reason}"),
            Self::UnknownCommand { input } => {
                write!(f, "Unknown command: {input}. Type 'help' for available commands.")
            }
            Self::ScriptReadFailed { path, source } => write!(f, "Failed to read script '{path}': {source}"),
            Self::Other { message } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectionFailed { source, .. } => Some(source),
            Self::RpcFailed { source } => Some(source),
            Self::ScriptReadFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<tonic::Status> for AdminError {
    fn from(source: tonic::Status) -> Self {
        Self::RpcFailed { source }
    }
}

impl From<tonic::transport::Error> for AdminError {
    fn from(source: tonic::transport::Error) -> Self {
        Self::ConnectionFailed { address: String::new(), source }
    }
}

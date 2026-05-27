/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use error::typedb_error;

typedb_error! {
    pub AdminError(component = "Admin", prefix = "ADM") {
        ConnectionFailed(1, "Failed to connect to '{address}'.", address: String, source: Arc<tonic::transport::Error>),
        RpcFailed(2, "{message}", message: String),
        InvalidArgCount(3, "Invalid number of arguments. Usage: {usage}", usage: String),
        InvalidArgument(4, "Invalid argument '{name}': {reason}", name: String, reason: String),
        UnknownCommand(5, "Unknown command: '{input}'. Type 'help' for available commands.", input: String),
        ScriptReadFailed(6, "Failed to read script '{path}'.", path: String, source: Arc<std::io::Error>),
        TokenFileUnreadable(7, "Could not read admin token file '{path}'.", path: String, source: Arc<std::io::Error>),
        TokenFileEmpty(8, "Admin token file '{path}' is empty.", path: String),
        TokenFilePermissionsTooWide(
            9,
            "Admin token file '{path}' has mode {mode:#o}; which exposes too wide permissions and a security danger. Run `chmod 600 '{path}'` or restart the server.",
            path: String,
            mode: u32,
        ),
        TokenFileNotRegular(10, "Admin token file '{path}' is not a regular file (symlinks, fifos and directories are refused).", path: String),
    }
}

impl From<tonic::Status> for AdminError {
    fn from(status: tonic::Status) -> Self {
        let code = status.code();
        let message = status.message();
        let formatted = if message.is_empty() { format!("{code}") } else { format!("{code}: {message}") };
        Self::RpcFailed { message: formatted }
    }
}

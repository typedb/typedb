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
        SocketPathInaccessible(7, "Admin socket '{path}' could not be inspected.", path: String, source: Arc<std::io::Error>),
        SocketNotASocket(8, "Admin endpoint at '{path}' is not a Unix socket; refusing to connect.", path: String),
        SocketPermissionsTooWide(
            9,
            "Admin socket '{path}' has mode {mode:#o}; permissions are too wide for a local-only admin endpoint. Run `chmod 600 '{path}'` or restart the server to recreate the socket.",
            path: String,
            mode: u32,
        ),
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

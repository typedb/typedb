/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::net::SocketAddr;
use std::sync::Arc;
use error::typedb_error;
use database::DatabaseOpenError;
use std::io;

typedb_error! {
    pub ServerOpenError(component = "Server open", prefix = "SRO") {
        NotADirectory(1, "Invalid path '{path}': not a directory.", path: String),
        CouldNotReadServerIDFile(2, "Could not read data from server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateServerIDFile(3, "Could not write data to server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateDataDirectory(4, "Could not create data directory in '{path}'.", path: String, source: Arc<io::Error>),
        InvalidServerID(5, "Server ID read from '{path}' is invalid. Delete the corrupted file and try again.", path: String),
        DatabaseOpen(6, "Could not open database.", typedb_source: DatabaseOpenError),
        Serve(7, "Could not serve on {address}.", address: SocketAddr, source: Arc<tonic::transport::Error>),
        MissingTLSCertificate(8, "TLS certificate path must be specified when encryption is enabled."),
        MissingTLSCertificateKey(9, "TLS certificate key path must be specified when encryption is enabled."),
        CouldNotReadTLSCertificate(10, "Could not read TLS certificate from '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotReadTLSCertificateKey(11, "Could not read TLS certificate key from '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotReadRootCA(12, "Could not read root CA from '{path}'.", path: String, source: Arc<io::Error>),
        TLSConfigError(13, "Failed to configure TLS.", source: Arc<tonic::transport::Error>),
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod command;
pub mod commands;
pub mod error;
pub mod repl;

use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc};

use server_admin_proto::type_db_admin_client::TypeDbAdminClient;
use tonic::{
    Request, Status,
    codegen::InterceptedService,
    metadata::MetadataValue,
    service::Interceptor,
    transport::{Channel, Endpoint},
};

use crate::error::AdminError;

pub type AdminClient = TypeDbAdminClient<InterceptedService<Channel, BearerTokenInterceptor>>;

#[derive(Clone)]
pub struct BearerTokenInterceptor {
    token: Arc<String>,
}

impl BearerTokenInterceptor {
    pub fn new(token: Arc<String>) -> Self {
        Self { token }
    }
}

impl Interceptor for BearerTokenInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let value: MetadataValue<_> = format!("Bearer {}", self.token)
            .parse()
            .map_err(|err| Status::internal(format!("Failed to build authorization header: {err}")))?;
        req.metadata_mut().insert("authorization", value);
        Ok(req)
    }
}

pub async fn connect(address: &str, token_path: &Path) -> Result<AdminClient, AdminError> {
    let channel = connect_channel(address).await?;
    let token = read_token_file(token_path)?;
    Ok(TypeDbAdminClient::with_interceptor(channel, BearerTokenInterceptor::new(Arc::new(token))))
}

pub async fn connect_channel(address: &str) -> Result<Channel, AdminError> {
    Endpoint::from_shared(format_insecure_address(address))
        .map_err(|source| AdminError::ConnectionFailed {
            address: address.to_string(),
            source: Arc::new(source.into()),
        })?
        .connect()
        .await
        .map_err(|source| AdminError::ConnectionFailed { address: address.to_string(), source: Arc::new(source) })
}

pub fn read_token_file(path: &Path) -> Result<String, AdminError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| AdminError::TokenFileUnreadable {
        path: path.display().to_string(),
        source: Arc::new(source),
    })?;
    if !metadata.file_type().is_file() {
        return Err(AdminError::TokenFileNotRegular { path: path.display().to_string() });
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(AdminError::TokenFilePermissionsTooWide { path: path.display().to_string(), mode });
    }

    let raw = std::fs::read_to_string(path).map_err(|source| AdminError::TokenFileUnreadable {
        path: path.display().to_string(),
        source: Arc::new(source),
    })?;
    let token = raw.trim().to_string();
    if token.is_empty() {
        return Err(AdminError::TokenFileEmpty { path: path.display().to_string() });
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("typedb-admin-token-test-{}-{name}", std::process::id()));
        let _ = fs::remove_file(&dir);
        dir
    }

    #[test]
    fn accepts_owner_only_token() {
        let path = temp_path("accept");
        fs::write(&path, "abc123\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let token = read_token_file(&path).expect("0600 token must be readable");
        assert_eq!(token, "abc123");
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn rejects_group_readable_token() {
        let path = temp_path("group");
        fs::write(&path, "secret").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        let err = read_token_file(&path).expect_err("0640 token must be rejected");
        match err {
            AdminError::TokenFilePermissionsTooWide { mode, .. } => assert_eq!(mode, 0o640),
            other => panic!("expected TokenFilePermissionsTooWide, got {other:?}"),
        }
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn rejects_world_readable_token() {
        let path = temp_path("world");
        fs::write(&path, "secret").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let err = read_token_file(&path).expect_err("0644 token must be rejected");
        match err {
            AdminError::TokenFilePermissionsTooWide { mode, .. } => assert_eq!(mode, 0o644),
            other => panic!("expected TokenFilePermissionsTooWide, got {other:?}"),
        }
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn rejects_symlink_token() {
        let actual = temp_path("symlink-target");
        let link = temp_path("symlink-link");
        fs::write(&actual, "secret").unwrap();
        fs::set_permissions(&actual, fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&actual, &link).unwrap();
        let err = read_token_file(&link).expect_err("symlink must be rejected");
        match err {
            AdminError::TokenFileNotRegular { .. } => {}
            other => panic!("expected TokenFileNotRegular, got {other:?}"),
        }
        fs::remove_file(&link).unwrap();
        fs::remove_file(&actual).unwrap();
    }
}

fn format_insecure_address(address: &str) -> String {
    format!("http://{address}")
}

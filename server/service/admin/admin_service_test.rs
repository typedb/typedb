/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use resource::{
    constants::server::{ADMIN_TOKEN_FILE_MODE, ADMIN_TOKEN_FILENAME, DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
    distribution_info::DistributionInfo,
};
use server::{
    ServerBuilder,
    admin_proto::{self, type_db_admin_client::TypeDbAdminClient},
    parameters::config::ConfigBuilder,
};
use test_utils::{TempDir, create_tmp_storage_dir};
use tokio::{
    runtime::Runtime,
    sync::{Mutex, OnceCell},
};
use tonic::{
    codegen::InterceptedService,
    transport::{Channel, Endpoint},
};
use typedb_admin::BearerTokenInterceptor;

const GRPC_ADDRESS: &str = "0.0.0.0:11729";
const GRPC_ADVERTISE_ADDRESS: &str = "127.0.0.1:11729";
const ADMIN_PORT: u16 = 11728;
const ADMIN_ADDRESS: &str = "http://127.0.0.1:11728";
const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

// Server task is spawned on this long-lived runtime so it outlives individual #[tokio::test]
// runtimes; otherwise the first test to win the OnceCell init would also tear the server
// down on its way out, breaking later tests with ConnectionReset.
static SERVER_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("admin-test-server")
        .build()
        .expect("failed to build admin-test server runtime")
});

static SERVER: OnceCell<(TempDir, tokio::sync::watch::Sender<()>, PathBuf)> = OnceCell::const_new();

// Serialises tests that mutate the admin user's password.
static MUTATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn config_path() -> PathBuf {
    std::env::current_dir().unwrap().join("server/config.yml")
}

async fn ensure_server_started() -> PathBuf {
    let (_dir, _sender, token_path) = SERVER
        .get_or_init(|| async {
            let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
            let server_dir = create_tmp_storage_dir();
            let token_path = server_dir.as_ref().join(ADMIN_TOKEN_FILENAME);

            let config = ConfigBuilder::from_file(config_path())
                .expect("Failed to load config file")
                .server_listen_address(GRPC_ADDRESS)
                .server_advertise_address(GRPC_ADVERTISE_ADDRESS)
                .server_http_enabled(false)
                .admin_port(ADMIN_PORT)
                .admin_enabled(true)
                .data_directory(server_dir.as_ref())
                .development_mode(true)
                .build()
                .expect("Failed to build config");

            let shutdown_sender_for_build = shutdown_sender.clone();
            let server = SERVER_RUNTIME
                .spawn(async move {
                    ServerBuilder::new()
                        .distribution_info(DISTRIBUTION_INFO)
                        .shutdown_channel((shutdown_sender_for_build, shutdown_receiver))
                        .build(config)
                        .await
                        .expect("Failed to build server")
                })
                .await
                .expect("Server build task panicked");

            SERVER_RUNTIME.spawn(async move {
                server.serve().await.expect("Server failed");
            });

            (server_dir, shutdown_sender, token_path)
        })
        .await;
    token_path.clone()
}

async fn admin_channel() -> Channel {
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        match Endpoint::from_static(ADMIN_ADDRESS).connect().await {
            Ok(ch) => return ch,
            Err(_) => continue,
        }
    }
    panic!("Failed to connect to admin endpoint");
}

async fn authenticated_admin_client(
    token_path: &Path,
) -> TypeDbAdminClient<InterceptedService<Channel, BearerTokenInterceptor>> {
    // Wait until the server has written the token file (it's the last step in serve_admin
    // setup, so its presence also signals the listener is up).
    for _ in 0..50 {
        if token_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let token = typedb_admin::read_token_file(token_path).expect("token file should be readable");
    let channel = admin_channel().await;
    TypeDbAdminClient::with_interceptor(channel, BearerTokenInterceptor::new(Arc::new(token)))
}

async fn unauthenticated_admin_client() -> TypeDbAdminClient<Channel> {
    let channel = admin_channel().await;
    TypeDbAdminClient::new(channel)
}

#[tokio::test]
async fn admin_server_version_with_token_succeeds() {
    let token_path = ensure_server_started().await;
    let mut client = authenticated_admin_client(&token_path).await;
    let response = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");
    let res = response.into_inner();
    assert_eq!(res.distribution, DISTRIBUTION_INFO.distribution);
    assert_eq!(res.version, DISTRIBUTION_INFO.version);
}

#[tokio::test]
async fn admin_server_version_without_token_is_rejected() {
    let _ = ensure_server_started().await;
    let mut client = unauthenticated_admin_client().await;
    let err = client
        .server_version(admin_proto::server_version::Req {})
        .await
        .expect_err("call without bearer token should be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "got {err:?}");
}

#[tokio::test]
async fn admin_server_version_with_wrong_token_is_rejected() {
    let _ = ensure_server_started().await;
    let channel = admin_channel().await;
    let bad = BearerTokenInterceptor::new(Arc::new("definitely-not-the-real-token".to_string()));
    let mut client = TypeDbAdminClient::with_interceptor(channel, bad);
    let err = client
        .server_version(admin_proto::server_version::Req {})
        .await
        .expect_err("call with wrong bearer token should be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "got {err:?}");
}

#[tokio::test]
async fn admin_token_file_has_owner_only_permissions() {
    let token_path = ensure_server_started().await;
    // Round-trip a call so we know the server's serve_admin has run to completion (and
    // therefore set up the file) before stat'ing it.
    let mut client = authenticated_admin_client(&token_path).await;
    let _ = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");

    let metadata = std::fs::symlink_metadata(&token_path).expect("token file should exist");
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, ADMIN_TOKEN_FILE_MODE,
        "Admin token file should be mode {:#o}, got {:#o}",
        ADMIN_TOKEN_FILE_MODE, mode
    );
}

#[tokio::test]
async fn users_set_password_succeeds_for_default_user() {
    let _guard = MUTATION_LOCK.lock().await;
    let token_path = ensure_server_started().await;
    let mut client = authenticated_admin_client(&token_path).await;

    client
        .users_set_password(admin_proto::users_set_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: "a-new-password".to_string(),
        })
        .await
        .expect("users_set_password RPC failed");

    // Restore the default so other tests sharing this server aren't affected.
    client
        .users_set_password(admin_proto::users_set_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: DEFAULT_USER_PASSWORD.to_string(),
        })
        .await
        .expect("users_set_password RPC failed during cleanup");
}

#[tokio::test]
async fn users_set_password_rejects_empty_username() {
    let token_path = ensure_server_started().await;
    let mut client = authenticated_admin_client(&token_path).await;
    let err = client
        .users_set_password(admin_proto::users_set_password::Req {
            username: String::new(),
            password: "anything".to_string(),
        })
        .await
        .expect_err("empty username should be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn users_set_password_rejects_empty_password() {
    let token_path = ensure_server_started().await;
    let mut client = authenticated_admin_client(&token_path).await;
    let err = client
        .users_set_password(admin_proto::users_set_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: String::new(),
        })
        .await
        .expect_err("empty password should be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn users_set_password_rejects_unknown_user() {
    let _guard = MUTATION_LOCK.lock().await;
    let token_path = ensure_server_started().await;
    let mut client = authenticated_admin_client(&token_path).await;
    let err = client
        .users_set_password(admin_proto::users_set_password::Req {
            username: "no-such-user".to_string(),
            password: "secret".to_string(),
        })
        .await
        .expect_err("unknown user should be rejected");
    assert_eq!(err.code(), tonic::Code::Internal);
    let message = err.message();
    assert!(
        message.contains("SRV4") || message.contains("User not found"),
        "expected user-not-found in error message, got: {message}",
    );
}

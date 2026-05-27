/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use resource::{
    constants::server::{ADMIN_DEFAULT_SOCKET_FILENAME, ADMIN_SOCKET_FILE_MODE, DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
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
use tonic::transport::Channel;

const GRPC_ADDRESS: &str = "0.0.0.0:11729";
const GRPC_ADVERTISE_ADDRESS: &str = "127.0.0.1:11729";
const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

// The server task must outlive the runtime of any individual #[tokio::test] that triggers
// the OnceCell init: tokio drops a Runtime's spawned tasks on Runtime::drop, so spawning
// the server onto the first test's runtime would kill it as soon as that test finished
// and any later test connecting to the socket would see ConnectionReset.
static SERVER_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("admin-test-server")
        .build()
        .expect("failed to build admin-test server runtime")
});

// Holds the temp dir and shutdown channel for the duration of the suite. The TempDir
// keeps the data on disk; if dropped, subsequent tests would observe an empty data dir.
static SERVER: OnceCell<(TempDir, tokio::sync::watch::Sender<()>, PathBuf)> = OnceCell::const_new();

// Serialises tests that mutate server state (the admin password). Read-only tests
// don't need it and can race freely.
static MUTATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn config_path() -> PathBuf {
    std::env::current_dir().unwrap().join("server/config.yml")
}

async fn ensure_server_started() -> PathBuf {
    let (_dir, _sender, socket_path) = SERVER
        .get_or_init(|| async {
            let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
            let server_dir = create_tmp_storage_dir();
            let socket_path = server_dir.as_ref().join(ADMIN_DEFAULT_SOCKET_FILENAME);

            // Build the config on the calling runtime (cheap, sync).
            let config = ConfigBuilder::from_file(config_path())
                .expect("Failed to load config file")
                .server_listen_address(GRPC_ADDRESS)
                .server_advertise_address(GRPC_ADVERTISE_ADDRESS)
                .server_http_enabled(false)
                .admin_enabled(true)
                .data_directory(server_dir.as_ref())
                .development_mode(true)
                .build()
                .expect("Failed to build config");

            // Build and run the server on the dedicated long-lived runtime so it
            // survives across individual #[tokio::test] runtimes.
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

            (server_dir, shutdown_sender, socket_path)
        })
        .await;
    socket_path.clone()
}

async fn admin_channel(socket_path: &Path) -> Channel {
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !socket_path.exists() {
            continue;
        }
        if let Ok(channel) = typedb_admin::connect_channel(socket_path).await {
            return channel;
        }
    }
    panic!("Failed to connect to admin Unix socket at {}", socket_path.display());
}

async fn connect_admin_client() -> TypeDbAdminClient<Channel> {
    let socket_path = ensure_server_started().await;
    TypeDbAdminClient::new(admin_channel(&socket_path).await)
}

#[tokio::test]
async fn admin_server_version() {
    let mut client = connect_admin_client().await;
    let response = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");
    let res = response.into_inner();
    assert_eq!(res.distribution, DISTRIBUTION_INFO.distribution);
    assert_eq!(res.version, DISTRIBUTION_INFO.version);
}

#[tokio::test]
async fn admin_server_status_reports_socket_path() {
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);
    let response = client.server_status(admin_proto::server_status::Req {}).await.expect("RPC failed");
    let res = response.into_inner();

    let grpc = res.grpc.expect("gRPC endpoint status should be present");
    assert!(!grpc.listen_address.is_empty(), "gRPC listen address should not be empty");
    assert!(grpc.advertise_address.is_some(), "gRPC advertise address should be some");
    assert!(!grpc.advertise_address.unwrap().is_empty(), "gRPC advertise address should not be empty");

    let reported = res.admin_address.expect("Admin address should be present");
    assert_eq!(
        Path::new(&reported),
        socket_path.as_path(),
        "Admin status should report the configured socket path",
    );
}

#[tokio::test]
async fn admin_socket_file_has_owner_only_permissions() {
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);
    // Round-trip a call so we know the listener is up before stat'ing the inode.
    let _ = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");

    let metadata = std::fs::symlink_metadata(&socket_path).expect("socket file should exist");
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, ADMIN_SOCKET_FILE_MODE,
        "Admin socket should be mode {:#o}, got {:#o}",
        ADMIN_SOCKET_FILE_MODE, mode
    );
}

#[tokio::test]
async fn users_set_password_succeeds_for_default_user() {
    let _guard = MUTATION_LOCK.lock().await;
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);

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
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);

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
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);

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
    let socket_path = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&socket_path).await);

    let err = client
        .users_set_password(admin_proto::users_set_password::Req {
            username: "no-such-user".to_string(),
            password: "secret".to_string(),
        })
        .await
        .expect_err("unknown user should be rejected");
    // Operator surfaces UserNotFound (typedb error code SRV4); the gRPC handler reports
    // it as Internal because the conversion from ArcServerStateError to tonic Status
    // doesn't carry a code yet.
    assert_eq!(err.code(), tonic::Code::Internal);
    let message = err.message();
    assert!(
        message.contains("SRV4") || message.contains("User not found"),
        "expected user-not-found in error message, got: {message}",
    );
}

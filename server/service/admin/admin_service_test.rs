/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

#[cfg(unix)]
use resource::constants::{common::PERMISSION_BITS_ALL, server::ADMIN_SOCKET_FILE_MODE};
use resource::{
    constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
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

#[cfg(unix)]
fn test_admin_endpoint(server_dir: &Path) -> String {
    server_dir.join("test-admin.sock").to_string_lossy().into_owned()
}

#[cfg(windows)]
fn test_admin_endpoint(_server_dir: &Path) -> String {
    format!(r"\\.\pipe\typedb_admin_test_{}", std::process::id())
}

#[cfg(unix)]
fn wrong_admin_endpoint() -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("typedb-admin-wrong-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p.to_string_lossy().into_owned()
}

#[cfg(windows)]
fn wrong_admin_endpoint() -> String {
    format!(r"\\.\pipe\typedb_admin_wrong_{}", std::process::id())
}

static SERVER_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("admin-test-server")
        .build()
        .expect("failed to build admin-test server runtime")
});

static SERVER: OnceCell<(TempDir, tokio::sync::watch::Sender<()>, String)> = OnceCell::const_new();

static MUTATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn config_path() -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let bazel_layout = cwd.join("server/config.yml");
    if bazel_layout.exists() { bazel_layout } else { cwd.join("config.yml") }
}

async fn ensure_server_started() -> String {
    let (_dir, _sender, endpoint) = SERVER
        .get_or_init(|| async {
            let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
            let server_dir = create_tmp_storage_dir();
            let endpoint = test_admin_endpoint(server_dir.as_ref());

            #[cfg(unix)]
            pre_create_stale_socket(&endpoint);

            let config = ConfigBuilder::from_file(config_path())
                .expect("Failed to load config file")
                .server_listen_address(GRPC_ADDRESS)
                .server_advertise_address(GRPC_ADVERTISE_ADDRESS)
                .server_http_enabled(false)
                .admin_enabled(true)
                .admin_socket_path(endpoint.clone())
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

            (server_dir, shutdown_sender, endpoint)
        })
        .await;
    endpoint.clone()
}

// Simulate a previous-run crash leaving a stale socket file at the
// path the server is about to bind. The server's bind logic must unlink the stale
// socket and bind successfully, otherwise restarts after a crash would be stuck
#[cfg(unix)]
fn pre_create_stale_socket(endpoint: &str) {
    use std::os::unix::net::UnixListener;
    let _ = std::fs::remove_file(endpoint);
    let stale = UnixListener::bind(endpoint).expect("pre-create stale socket");
    drop(stale);
}

async fn admin_channel(endpoint: &str) -> Channel {
    let path = Path::new(endpoint);
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if let Ok(channel) = typedb_admin::connect_channel(path).await {
            return channel;
        }
    }
    panic!("Failed to connect to admin endpoint {endpoint}");
}

async fn connect_admin_client() -> TypeDbAdminClient<Channel> {
    let endpoint = ensure_server_started().await;
    TypeDbAdminClient::new(admin_channel(&endpoint).await)
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
async fn admin_server_status_reports_endpoint() {
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);
    let response = client.server_status(admin_proto::server_status::Req {}).await.expect("RPC failed");
    let res = response.into_inner();

    let grpc = res.grpc.expect("gRPC endpoint status should be present");
    assert!(!grpc.listen_address.is_empty(), "gRPC listen address should not be empty");
    assert!(grpc.advertise_address.is_some(), "gRPC advertise address should be some");
    assert!(!grpc.advertise_address.unwrap().is_empty(), "gRPC advertise address should not be empty");

    let reported = res.admin_address.expect("Admin endpoint should be reported in status");
    assert_eq!(reported, endpoint, "Admin status should report the configured endpoint");
}

// Unix-only: the bound socket file has the documented mode bits. Windows uses a DACL
// whose verification would need GetSecurityInfo and is left as a manual concern
#[cfg(unix)]
#[tokio::test]
async fn admin_socket_file_has_owner_only_permissions() {
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);
    let _ = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");

    let metadata = std::fs::symlink_metadata(&endpoint).expect("socket file should exist");
    let mode = metadata.permissions().mode() & PERMISSION_BITS_ALL;
    assert_eq!(
        mode, ADMIN_SOCKET_FILE_MODE,
        "Admin socket should be mode {:#o}, got {:#o}",
        ADMIN_SOCKET_FILE_MODE, mode
    );
}

// Implicit positive control. If the server fails to recover from the stale-socket
// pre-create in `pre_create_stale_socket`, this test fails to connect
#[tokio::test]
async fn correct_endpoint_connects_and_admins_successfully() {
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);

    let response = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");
    assert_eq!(response.into_inner().distribution, DISTRIBUTION_INFO.distribution);
}

#[tokio::test]
async fn wrong_endpoint_does_not_connect() {
    let _real = ensure_server_started().await;

    let bad = wrong_admin_endpoint();
    let bad_path = Path::new(&bad);
    let result = typedb_admin::connect_channel(bad_path).await;
    assert!(result.is_err(), "connect to wrong endpoint {bad:?} must fail; got: {result:?}");
}

#[tokio::test]
async fn user_reset_password_succeeds_for_default_user() {
    let _guard = MUTATION_LOCK.lock().await;
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);

    client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: "a-new-password".to_string(),
        })
        .await
        .expect("user_reset_password RPC failed");

    client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: DEFAULT_USER_PASSWORD.to_string(),
        })
        .await
        .expect("user_reset_password RPC failed during cleanup");
}

#[tokio::test]
async fn user_reset_password_rejects_empty_username() {
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);
    let err = client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: String::new(),
            password: "anything".to_string(),
        })
        .await
        .expect_err("empty username should be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn user_reset_password_rejects_empty_password() {
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);
    let err = client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: String::new(),
        })
        .await
        .expect_err("empty password should be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn user_reset_password_accepts_special_characters() {
    let _guard = MUTATION_LOCK.lock().await;
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);

    let special = r#"P@ss w0rd!#$%^&*()'"\"#;
    client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: special.to_string(),
        })
        .await
        .expect("special-char password should be accepted");

    client
        .user_reset_password(admin_proto::user_reset_password::Req {
            username: DEFAULT_USER_NAME.to_string(),
            password: DEFAULT_USER_PASSWORD.to_string(),
        })
        .await
        .expect("cleanup reset failed");
}

#[tokio::test]
async fn user_reset_password_rejects_unknown_user() {
    let _guard = MUTATION_LOCK.lock().await;
    let endpoint = ensure_server_started().await;
    let mut client = TypeDbAdminClient::new(admin_channel(&endpoint).await);
    let err = client
        .user_reset_password(admin_proto::user_reset_password::Req {
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

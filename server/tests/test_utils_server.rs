/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};

use concurrency::TokioTaskTracker;
use database::database_manager::ImportOwnership;
use resource::distribution_info::DistributionInfo;
use server::{
    ServerBuilder,
    audit::Auditor,
    error::ServerOpenError,
    parameters::config::{ConfigBuilder, DiagnosticsConfig},
    state::ServerState,
};
use test_utils::{TempDir, create_tmp_storage_dir};
use tokio::{
    net::TcpStream,
    sync::{Mutex, MutexGuard, watch},
    task::JoinHandle,
};

const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

static SERVER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub struct TestServer {
    _dir: TempDir,
    _lock: MutexGuard<'static, ()>,
    shutdown_sender: watch::Sender<()>,
    serving: JoinHandle<Result<(), ServerOpenError>>,
    pub grpc_address: SocketAddr,
    pub http_address: SocketAddr,
}

impl TestServer {
    pub async fn start(
        auditor: Arc<dyn Auditor>,
        configure: impl FnOnce(ConfigBuilder, &Path) -> ConfigBuilder,
    ) -> Self {
        let lock = SERVER_LOCK.lock().await;
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let dir = create_tmp_storage_dir();
        let [grpc_address, http_address] = free_addresses();
        let config_builder = ConfigBuilder::from_file(config_path())
            .expect("Failed to load config file")
            .server_listen_address(grpc_address.to_string())
            .server_advertise_address(grpc_address.to_string())
            .server_http_enabled(true)
            .server_http_listen_address(http_address.to_string())
            .server_http_advertise_address(format!("http://{http_address}"))
            .admin_enabled(false)
            .data_directory(dir.as_ref())
            .development_mode(true)
            .diagnostics(DiagnosticsConfig::disabled());
        let config = configure(config_builder, dir.as_ref()).build().expect("Failed to build config");

        let background_tasks_tracker = TokioTaskTracker::new(shutdown_receiver.clone());
        let mut server_builder = ServerBuilder::new();
        let server_id = server_builder.initialise_storage(&config.storage).expect("Failed to initialise storage");
        let server_state = ServerState::new(
            DISTRIBUTION_INFO,
            config.clone(),
            server_id.to_owned(),
            None,
            ImportOwnership::Exclusive,
            shutdown_receiver.clone(),
            background_tasks_tracker.get_spawner(),
        )
        .await
        .expect("Failed to prepare server state")
        .auditor(auditor)
        .build();
        server_state.initialise().await.expect("Failed to initialise server state");

        let server = server_builder
            .distribution_info(DISTRIBUTION_INFO)
            .shutdown_channel((shutdown_sender.clone(), shutdown_receiver))
            .background_tasks_tracker(background_tasks_tracker)
            .server_state(Arc::new(server_state))
            .build(config)
            .await
            .expect("Failed to build server");
        let serving = tokio::spawn(server.serve());

        for address in [grpc_address, http_address] {
            let mut attempts = 0;
            while TcpStream::connect(address).await.is_err() {
                assert!(!serving.is_finished(), "Server stopped before listening on {address}");
                attempts += 1;
                assert!(attempts < 100, "Server did not start listening on {address}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Self { _dir: dir, _lock: lock, shutdown_sender, serving, grpc_address, http_address }
    }

    pub async fn stop(self) {
        let _ = self.shutdown_sender.send(());
        tokio::time::timeout(Duration::from_secs(30), self.serving)
            .await
            .expect("Server did not stop within 30 seconds")
            .expect("Server task panicked")
            .expect("Server failed");
    }
}

fn config_path() -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let bazel_layout = cwd.join("server/config.yml");
    if bazel_layout.exists() { bazel_layout } else { cwd.join("config.yml") }
}

fn free_addresses() -> [SocketAddr; 2] {
    let listeners = [TcpListener::bind("127.0.0.1:0").unwrap(), TcpListener::bind("127.0.0.1:0").unwrap()];
    listeners.map(|listener| listener.local_addr().unwrap())
}

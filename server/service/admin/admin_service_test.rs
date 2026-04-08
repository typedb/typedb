/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use resource::distribution_info::DistributionInfo;
use server::{
    admin_proto::{self, type_db_admin_client::TypeDbAdminClient},
    parameters::config::ConfigBuilder,
    ServerBuilder,
};
use test_utils::create_tmp_storage_dir;

const GRPC_ADDRESS: &str = "127.0.0.1:11729";
const ADMIN_PORT: u16 = 11728;
const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

fn config_path() -> PathBuf {
    std::env::current_dir().unwrap().join("server/config.yml")
}

#[tokio::test]
async fn admin_server_version() {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
    let server_dir = create_tmp_storage_dir();
    let config = ConfigBuilder::from_file(config_path())
        .expect("Failed to load config file")
        .server_address(GRPC_ADDRESS)
        .server_http_enabled(false)
        .admin_port(ADMIN_PORT)
        .admin_enabled(true)
        .data_directory(server_dir.as_ref())
        .development_mode(true)
        .build()
        .expect("Failed to build config");

    let server = ServerBuilder::new()
        .distribution_info(DISTRIBUTION_INFO)
        .shutdown_channel((shutdown_sender.clone(), shutdown_receiver))
        .build(config)
        .await
        .expect("Failed to build server");

    let server_handle = tokio::spawn(async move {
        server.serve().await.expect("Server failed");
    });

    let mut client = None;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        match TypeDbAdminClient::connect(format!("http://{ADMIN_PORT}")).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) => continue,
        }
    }
    let mut client = client.expect("Failed to connect to admin service");

    let response = client.server_version(admin_proto::server_version::Req {}).await.expect("RPC failed");
    let res = response.into_inner();
    assert_eq!(res.distribution, DISTRIBUTION_INFO.distribution);
    assert_eq!(res.version, DISTRIBUTION_INFO.version);

    shutdown_sender.send(()).expect("Failed to send shutdown signal");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), server_handle).await;
}

mod localhost_guard_tests {
    use std::net::SocketAddr;

    use server::service::admin::localhost_guard::is_loopback;

    #[test]
    fn loopback_ipv4_allowed() {
        let addr: SocketAddr = "127.0.0.1:1728".parse().unwrap();
        assert!(is_loopback(addr));
    }

    #[test]
    fn loopback_ipv6_allowed() {
        let addr: SocketAddr = "[::1]:1728".parse().unwrap();
        assert!(is_loopback(addr));
    }

    #[test]
    fn non_loopback_rejected() {
        let addr: SocketAddr = "192.168.1.1:1728".parse().unwrap();
        assert!(!is_loopback(addr));
    }

    #[test]
    fn non_loopback_public_rejected() {
        let addr: SocketAddr = "8.8.8.8:1728".parse().unwrap();
        assert!(!is_loopback(addr));
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use resource::distribution_info::DistributionInfo;
use server::{
    ServerBuilder,
    admin_proto::{self, type_db_admin_client::TypeDbAdminClient},
    parameters::config::ConfigBuilder,
};
use test_utils::{TempDir, create_tmp_storage_dir};
use tokio::sync::OnceCell;

const GRPC_ADDRESS: &str = "127.0.0.1:11729";
const ADMIN_PORT: u16 = 11728;
const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

static SERVER: OnceCell<(TempDir, tokio::sync::watch::Sender<()>)> = OnceCell::const_new();

fn config_path() -> PathBuf {
    std::env::current_dir().unwrap().join("server/config.yml")
}

async fn ensure_server_started() {
    SERVER
        .get_or_init(|| async {
            let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
            let server_dir = create_tmp_storage_dir();
            let config = ConfigBuilder::from_file(config_path())
                .expect("Failed to load config file")
                .server_listen_address(GRPC_ADDRESS)
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

            tokio::spawn(async move {
                server.serve().await.expect("Server failed");
            });

            (server_dir, shutdown_sender)
        })
        .await;
}

async fn connect_admin_client() -> TypeDbAdminClient<tonic::transport::Channel> {
    ensure_server_started().await;
    let mut client = None;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        match TypeDbAdminClient::connect(format!("http://127.0.0.1:{ADMIN_PORT}")).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) => continue,
        }
    }
    client.expect("Failed to connect to admin service")
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
async fn admin_server_status() {
    let mut client = connect_admin_client().await;
    let response = client.server_status(admin_proto::server_status::Req {}).await.expect("RPC failed");
    let res = response.into_inner();

    let grpc = res.grpc.expect("gRPC endpoint status should be present");
    assert!(!grpc.listen_address.is_empty(), "gRPC listen address should not be empty");
    assert!(!grpc.advertise_address.is_empty(), "gRPC advertise address should not be empty");

    assert!(res.http.is_none(), "HTTP should be disabled in test config");

    let admin_address = res.admin_address.expect("Admin address should be present");
    assert!(admin_address.contains(&ADMIN_PORT.to_string()), "Admin address should contain the configured port");
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

mod localhost_guard_middleware_tests {
    use std::{
        net::SocketAddr,
        task::{Context, Poll},
    };

    use futures::future::BoxFuture;
    use http::{Request, Response};
    use server::service::admin::localhost_guard::LocalhostGuardLayer;
    use tonic::{Status, body::BoxBody, transport::server::TcpConnectInfo};
    use tower::{Layer, Service};

    #[derive(Clone)]
    struct MockService;

    impl Service<Request<BoxBody>> for MockService {
        type Response = Response<BoxBody>;
        type Error = Status;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<BoxBody>) -> Self::Future {
            Box::pin(async { Ok(Response::new(BoxBody::default())) })
        }
    }

    fn request_with_remote_addr(addr: SocketAddr) -> Request<BoxBody> {
        let mut request = Request::new(BoxBody::default());
        let connect_info =
            TcpConnectInfo { local_addr: Some("127.0.0.1:1728".parse().unwrap()), remote_addr: Some(addr) };
        request.extensions_mut().insert(connect_info);
        request
    }

    #[tokio::test]
    async fn loopback_connection_allowed() {
        let mut service = LocalhostGuardLayer.layer(MockService);
        let request = request_with_remote_addr("127.0.0.1:54321".parse().unwrap());
        let result = service.call(request).await;
        assert!(result.is_ok(), "Loopback connection should be allowed");
    }

    #[tokio::test]
    async fn non_loopback_connection_rejected() {
        let mut service = LocalhostGuardLayer.layer(MockService);
        let request = request_with_remote_addr("192.168.1.100:54321".parse().unwrap());
        let result = service.call(request).await;
        assert!(result.is_err(), "Non-loopback connection should be rejected");
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn missing_connect_info_rejected() {
        let mut service = LocalhostGuardLayer.layer(MockService);
        let request = Request::new(BoxBody::default()); // no TcpConnectInfo
        let result = service.call(request).await;
        assert!(result.is_err(), "Missing connect info should be rejected");
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }
}

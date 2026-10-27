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

use resource::distribution_info::DistributionInfo;
use server::{
    ServerBuilder,
    error::ServerOpenError,
    parameters::config::{ConfigBuilder, DiagnosticsConfig, EncryptionConfig},
};
use test_utils::{TempDir, create_tmp_storage_dir};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, RootCertStore,
        crypto::ring::default_provider,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    },
};

const HTTP2_SETTINGS_FRAME_TYPE: u8 = 0x4;

const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

// Test-only certificates (EC P-256, valid for 100 years): a CA, a server certificate for 'localhost'
// and a client certificate signed by it, and a client certificate signed by an unrelated CA.
const CA_CERTIFICATE: &str = r"-----BEGIN CERTIFICATE-----
MIIBmjCCAT+gAwIBAgIUeXX3y2t3srD1KO4q/Ywv3akJB9kwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOVHlwZURCIHRlc3QgQ0EwIBcNMjYxMDI0MTE1MTQ2WhgPMjEy
NjA5MzAxMTUxNDZaMBkxFzAVBgNVBAMMDlR5cGVEQiB0ZXN0IENBMFkwEwYHKoZI
zj0CAQYIKoZIzj0DAQcDQgAEsaqmk8pEQ/L1A1umD3SL6prNHv0PtQl3zfDRG4+n
LkFGPfXKgK6qh1RxSA47ycTyfBhz4AVFDc7cIxlnZ23zOqNjMGEwHQYDVR0OBBYE
FJJenktDHldT1kKlwMmTsmgVju6MMB8GA1UdIwQYMBaAFJJenktDHldT1kKlwMmT
smgVju6MMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgIEMAoGCCqGSM49
BAMCA0kAMEYCIQCVXDj4G3TIzyycOLyuMMhKSOJ9MA1iEQW5Cnxov78fbwIhAMqr
QGITAj95W8HgJUPQB0k1cQckY3/CYQL8z2EKglov
-----END CERTIFICATE-----
";
const SERVER_CERTIFICATE: &str = r"-----BEGIN CERTIFICATE-----
MIIBhjCCASygAwIBAgIJAOFKz6nAUso4MAoGCCqGSM49BAMCMBkxFzAVBgNVBAMM
DlR5cGVEQiB0ZXN0IENBMCAXDTI2MTAyNDExNTE0NloYDzIxMjYwOTMwMTE1MTQ2
WjARMQ8wDQYDVQQDDAZzZXJ2ZXIwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAR8
evgpckA47ayvKObDgQNmo/QGCqvERH7QzTOetMCvgF3nQH19Y1R7eBmCZsf0SQGC
iRvept3ExEML5CtDhy5Vo2MwYTAUBgNVHREEDTALgglsb2NhbGhvc3QwCQYDVR0T
BAIwADAdBgNVHQ4EFgQUi+nTDxXwGzpYJfCxs/Lg2n+N98gwHwYDVR0jBBgwFoAU
kl6eS0MeV1PWQqXAyZOyaBWO7owwCgYIKoZIzj0EAwIDSAAwRQIhAOx/pEZWLwSm
SR6cJUoC/aBd2kums8nOjrQ13q6UXKE8AiAYZYMPlUuPvFK8dvq2t+ebZuL9H5Cw
j4E1xCbvELY5Jg==
-----END CERTIFICATE-----
";
const SERVER_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgIu7frd6r4IP7YPHt
w0bcgaM0bbSmYXkFQk98z2rwRRqhRANCAAR8evgpckA47ayvKObDgQNmo/QGCqvE
RH7QzTOetMCvgF3nQH19Y1R7eBmCZsf0SQGCiRvept3ExEML5CtDhy5V
-----END PRIVATE KEY-----
";
const CLIENT_CERTIFICATE: &str = r"-----BEGIN CERTIFICATE-----
MIIBgzCCASmgAwIBAgIJAJC+1oxb+cNJMAoGCCqGSM49BAMCMBkxFzAVBgNVBAMM
DlR5cGVEQiB0ZXN0IENBMCAXDTI2MTAyNDExNTE0NloYDzIxMjYwOTMwMTE1MTQ2
WjARMQ8wDQYDVQQDDAZjbGllbnQwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATC
MmlSIzg5z6paXJ5fNTOOjVGlX5oZeFbfg/VbSxm3DhsDzk4fdvrsr6XMHZmoY619
9LjlVkkbcMcol0uFqBJfo2AwXjARBgNVHREECjAIggZjbGllbnQwCQYDVR0TBAIw
ADAdBgNVHQ4EFgQU6P27Vd8bx4cHDIPpNQ1UV4vrhUgwHwYDVR0jBBgwFoAUkl6e
S0MeV1PWQqXAyZOyaBWO7owwCgYIKoZIzj0EAwIDSAAwRQIgNoN3yA8xmbwI/gIB
hc1NJaM9djGwneaWEI8Gmx7mUs0CIQCJEW7n3TbAkyj2jMNdUP4cmEbXW2NpFc5i
GsiWq+92EA==
-----END CERTIFICATE-----
";
const CLIENT_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg0yf6XjsVaFlwWm+m
lf91GC3ONCTk4LP5kXmpC2CWIG+hRANCAATCMmlSIzg5z6paXJ5fNTOOjVGlX5oZ
eFbfg/VbSxm3DhsDzk4fdvrsr6XMHZmoY6199LjlVkkbcMcol0uFqBJf
-----END PRIVATE KEY-----
";
const FOREIGN_CLIENT_CERTIFICATE: &str = r"-----BEGIN CERTIFICATE-----
MIIBkTCCATigAwIBAgIIH0UlCWQWm9cwCgYIKoZIzj0EAwIwITEfMB0GA1UEAwwW
VHlwZURCIHRlc3QgZm9yZWlnbiBDQTAgFw0yNjEwMjQxMTUxNDZaGA8yMTI2MDkz
MDExNTE0NlowGTEXMBUGA1UEAwwOZm9yZWlnbi1jbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAAQ8ugpMgZ9M1qrLezvW7UzqMfWZFEjbe8TJqwxNtKJteno5
7TQvOB+oMwbLEF8hIdveMgU+6imkY0m2/JfMM2Tzo2AwXjARBgNVHREECjAIggZj
bGllbnQwCQYDVR0TBAIwADAdBgNVHQ4EFgQUwPsjwJHZtzXICnxFKBLZVXphbhAw
HwYDVR0jBBgwFoAULCwpqE40qBy/r6dNUERa/3dq7TswCgYIKoZIzj0EAwIDRwAw
RAIgKStUBM4ErD8W1+zFwxSria4rnou0m/tzhsiG6MfVCL8CIDqKvkYLzN4zwyyH
iKPV7iDXS/+AxrlYd+J1KVx4X4kH
-----END CERTIFICATE-----
";
const FOREIGN_CLIENT_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgqiMeIW/cpGQP6Qx8
68LTu+fOn7jG1BFC/8EYL+Mfw7mhRANCAAQ8ugpMgZ9M1qrLezvW7UzqMfWZFEjb
e8TJqwxNtKJteno57TQvOB+oMwbLEF8hIdveMgU+6imkY0m2/JfMM2Tz
-----END PRIVATE KEY-----
";

#[derive(Debug, Clone, Copy)]
enum Endpoint {
    Grpc,
    Http,
}

const ENDPOINTS: [Endpoint; 2] = [Endpoint::Grpc, Endpoint::Http];

static SERVER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct TestServer {
    _dir: TempDir,
    shutdown_sender: watch::Sender<()>,
    serving: JoinHandle<Result<(), ServerOpenError>>,
    grpc_address: SocketAddr,
    http_address: SocketAddr,
}

impl TestServer {
    async fn start(with_ca_certificate: bool) -> Self {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let dir = create_tmp_storage_dir();
        let [grpc_address, http_address] = free_addresses();
        let encryption = EncryptionConfig {
            enabled: true,
            certificate: Some(write_file(dir.as_ref(), "server.pem", SERVER_CERTIFICATE)),
            certificate_key: Some(write_file(dir.as_ref(), "server-key.pem", SERVER_KEY)),
            ca_certificate: with_ca_certificate.then(|| write_file(dir.as_ref(), "ca.pem", CA_CERTIFICATE)),
        };
        let config = ConfigBuilder::from_file(config_path())
            .expect("Failed to load config file")
            .server_listen_address(grpc_address.to_string())
            .server_advertise_address(grpc_address.to_string())
            .server_http_enabled(true)
            .server_http_listen_address(http_address.to_string())
            .server_http_advertise_address(format!("https://{http_address}"))
            .admin_enabled(false)
            .encryption(encryption)
            .data_directory(dir.as_ref())
            .development_mode(true)
            .diagnostics(DiagnosticsConfig::disabled())
            .build()
            .expect("Failed to build config");
        let server = ServerBuilder::new()
            .distribution_info(DISTRIBUTION_INFO)
            .shutdown_channel((shutdown_sender.clone(), shutdown_receiver))
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
        Self { _dir: dir, shutdown_sender, serving, grpc_address, http_address }
    }

    async fn stop(self) {
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

fn write_file(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

fn client_config(endpoint: Endpoint, client_identity: Option<(&str, &str)>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(CA_CERTIFICATE.as_bytes()).unwrap()).unwrap();
    let builder = ClientConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let mut config = match client_identity {
        None => builder.with_no_client_auth(),
        Some((certificate, key)) => builder
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_slice(certificate.as_bytes()).unwrap()],
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap(),
    };
    config.alpn_protocols = match endpoint {
        Endpoint::Grpc => vec![b"h2".to_vec()],
        Endpoint::Http => vec![b"http/1.1".to_vec()],
    };
    config
}

async fn connect(server: &TestServer, endpoint: Endpoint, client_identity: Option<(&str, &str)>) -> Result<(), String> {
    let address = match endpoint {
        Endpoint::Grpc => server.grpc_address,
        Endpoint::Http => server.http_address,
    };
    let connector = TlsConnector::from(Arc::new(client_config(endpoint, client_identity)));
    let tcp = TcpStream::connect(address).await.map_err(|err| err.to_string())?;
    let mut stream =
        connector.connect(ServerName::try_from("localhost").unwrap(), tcp).await.map_err(|err| err.to_string())?;
    match endpoint {
        Endpoint::Grpc => {
            stream.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.map_err(|err| err.to_string())?;
            let mut frame_header = [0u8; 9];
            stream.read_exact(&mut frame_header).await.map_err(|err| err.to_string())?;
            if frame_header[3] != HTTP2_SETTINGS_FRAME_TYPE {
                return Err(format!("Unexpected HTTP/2 frame header: {frame_header:?}"));
            }
        }
        Endpoint::Http => {
            stream
                .write_all(b"GET /v1/version HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .map_err(|err| err.to_string())?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.map_err(|err| err.to_string())?;
            let response = String::from_utf8_lossy(&response);
            if !response.starts_with("HTTP/1.1 200") {
                return Err(format!("Unexpected response: {response}"));
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ca_certificate_accepts_clients_without_a_certificate() {
    let _guard = SERVER_LOCK.lock().await;
    let server = TestServer::start(true).await;
    for endpoint in ENDPOINTS {
        let result = connect(&server, endpoint, None).await;
        assert!(result.is_ok(), "{endpoint:?}: expected a client without a certificate to be accepted, got {result:?}");
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ca_certificate_accepts_client_certificates_it_signed() {
    let _guard = SERVER_LOCK.lock().await;
    let server = TestServer::start(true).await;
    for endpoint in ENDPOINTS {
        let result = connect(&server, endpoint, Some((CLIENT_CERTIFICATE, CLIENT_KEY))).await;
        assert!(
            result.is_ok(),
            "{endpoint:?}: expected a client certificate signed by the CA to be accepted, got {result:?}"
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ca_certificate_rejects_client_certificates_it_did_not_sign() {
    let _guard = SERVER_LOCK.lock().await;
    let server = TestServer::start(true).await;
    for endpoint in ENDPOINTS {
        let result = connect(&server, endpoint, Some((FOREIGN_CLIENT_CERTIFICATE, FOREIGN_CLIENT_KEY))).await;
        assert!(
            result.as_ref().is_err_and(|err| err.contains("UnknownCA")),
            "{endpoint:?}: expected a client certificate from another CA to be rejected as unknown, got {result:?}"
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_ca_certificate_ignores_client_certificates() {
    let _guard = SERVER_LOCK.lock().await;
    let server = TestServer::start(false).await;
    for endpoint in ENDPOINTS {
        let result = connect(&server, endpoint, Some((FOREIGN_CLIENT_CERTIFICATE, FOREIGN_CLIENT_KEY))).await;
        assert!(
            result.is_ok(),
            "{endpoint:?}: expected client certificates to be ignored without a CA, got {result:?}"
        );
    }
    server.stop().await;
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs, path::PathBuf, sync::Arc};

use server::{
    parameters::config::EncryptionConfig,
    service::{grpc, http},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    rustls::{
        ClientConfig, RootCertStore,
        crypto::ring::default_provider,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    },
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{service::Routes, transport::Server};

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

struct Fixtures {
    dir: PathBuf,
}

impl Fixtures {
    fn new(test_name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("typedb-tls-{test_name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn encryption_config(&self, with_ca: bool) -> EncryptionConfig {
        EncryptionConfig {
            enabled: true,
            certificate: Some(self.write("server.pem", SERVER_CERTIFICATE)),
            certificate_key: Some(self.write("server-key.pem", SERVER_KEY)),
            ca_certificate: with_ca.then(|| self.write("ca.pem", CA_CERTIFICATE)),
        }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.join(name);
        fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn client_config(client_identity: Option<(&str, &str)>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(CA_CERTIFICATE.as_bytes()).unwrap()).unwrap();
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let mut config = match client_identity {
        None => builder.with_no_client_auth(),
        Some((certificate, key)) => builder
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_slice(certificate.as_bytes()).unwrap()],
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap(),
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

async fn connect(
    endpoint: Endpoint,
    encryption_config: &EncryptionConfig,
    client_identity: Option<(&str, &str)>,
) -> Result<(), String> {
    let _ = default_provider().install_default();
    let client_config = Arc::new(client_config(client_identity));
    match endpoint {
        Endpoint::Http => {
            let server_config = http::encryption::prepare_tls_config(encryption_config).unwrap().unwrap();
            let (client_io, server_io) = duplex(64 * 1024);
            tokio::spawn(async move {
                if let Ok(mut stream) = TlsAcceptor::from(Arc::new(server_config)).accept(server_io).await {
                    let _ = stream.write_all(b"ready").await;
                    let _ = stream.flush().await;
                    let _ = stream.read(&mut [0u8; 1]).await;
                }
            });
            exchange(client_config, client_io).await
        }
        Endpoint::Grpc => {
            let tls_config = grpc::encryption::prepare_tls_config(encryption_config).unwrap().unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(
                Server::builder()
                    .tls_config(tls_config)
                    .unwrap()
                    .add_routes(Routes::default())
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            );
            let result = exchange(client_config, TcpStream::connect(address).await.unwrap()).await;
            server.abort();
            result
        }
    }
}

async fn exchange(client_config: Arc<ClientConfig>, io: impl AsyncRead + AsyncWrite + Unpin) -> Result<(), String> {
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut stream = TlsConnector::from(client_config).connect(server_name, io).await.map_err(|err| err.to_string())?;
    // The HTTP/2 client preface; the gRPC server answers with its SETTINGS frame.
    stream.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.map_err(|err| err.to_string())?;
    let mut buffer = [0u8; 5];
    stream.read_exact(&mut buffer).await.map_err(|err| err.to_string())?;
    Ok(())
}

#[tokio::test]
async fn ca_certificate_accepts_clients_without_a_certificate() {
    let fixtures = Fixtures::new("no-client-certificate");
    for endpoint in ENDPOINTS {
        let result = connect(endpoint, &fixtures.encryption_config(true), None).await;
        assert!(result.is_ok(), "{endpoint:?}: expected a client without a certificate to be accepted, got {result:?}");
    }
}

#[tokio::test]
async fn ca_certificate_accepts_client_certificates_it_signed() {
    let fixtures = Fixtures::new("trusted-client-certificate");
    for endpoint in ENDPOINTS {
        let result = connect(endpoint, &fixtures.encryption_config(true), Some((CLIENT_CERTIFICATE, CLIENT_KEY))).await;
        assert!(
            result.is_ok(),
            "{endpoint:?}: expected a client certificate signed by the CA to be accepted, got {result:?}"
        );
    }
}

#[tokio::test]
async fn ca_certificate_rejects_client_certificates_it_did_not_sign() {
    let fixtures = Fixtures::new("foreign-client-certificate");
    for endpoint in ENDPOINTS {
        let result = connect(
            endpoint,
            &fixtures.encryption_config(true),
            Some((FOREIGN_CLIENT_CERTIFICATE, FOREIGN_CLIENT_KEY)),
        )
        .await;
        assert!(
            result.as_ref().is_err_and(|err| err.contains("UnknownCA")),
            "{endpoint:?}: expected a client certificate from another CA to be rejected as unknown, got {result:?}"
        );
    }
}

#[tokio::test]
async fn no_ca_certificate_ignores_client_certificates() {
    let fixtures = Fixtures::new("no-ca-certificate");
    for endpoint in ENDPOINTS {
        let result = connect(
            endpoint,
            &fixtures.encryption_config(false),
            Some((FOREIGN_CLIENT_CERTIFICATE, FOREIGN_CLIENT_KEY)),
        )
        .await;
        assert!(
            result.is_ok(),
            "{endpoint:?}: expected client certificates to be ignored without a CA, got {result:?}"
        );
    }
}

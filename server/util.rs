use std::net::SocketAddr;
use tokio::net::lookup_host;

pub async fn resolve_address(address: String) -> SocketAddr {
    lookup_host(address.clone())
        .await
        .unwrap()
        .next()
        .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
}
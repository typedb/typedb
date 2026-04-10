/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    net::SocketAddr,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use http::Request;
use tonic::{body::BoxBody, transport::server::TcpConnectInfo, Status};
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub struct LocalhostGuardLayer;

impl<S: Clone> Layer<S> for LocalhostGuardLayer {
    type Service = LocalhostGuard<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LocalhostGuard { inner }
    }
}

#[derive(Debug, Clone)]
pub struct LocalhostGuard<S> {
    inner: S,
}

impl<S> Service<Request<BoxBody>> for LocalhostGuard<S>
where
    S: Service<Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<Status>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let remote_addr = request.extensions().get::<TcpConnectInfo>().and_then(|info| info.remote_addr());
            match remote_addr {
                Some(addr) if is_loopback(addr) => inner.call(request).await,
                Some(_) => Err(Status::permission_denied("Admin service is only accessible from localhost").into()),
                None => {
                    Err(Status::permission_denied("Admin service requires connection info to verify localhost access")
                        .into())
                }
            }
        })
    }
}

pub fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

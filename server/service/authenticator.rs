/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use std::task::{Context, Poll};
use futures::future::BoxFuture;
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};
use http::Request;
use tokio::task::spawn_blocking;
use crate::service::state::ServerState;

#[derive(Clone, Debug)]
pub struct Authenticator {
    server_state: Arc<ServerState>
}

impl Authenticator {
    pub(crate) fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }

    pub fn authenticate(&self, http: Request<BoxBody>) -> Result<Request<BoxBody>, Status> {
        self.server_state.authenticate(http)
    }
}

impl<S: Clone> Layer<S> for Authenticator {
    type Service = AuthenticatedService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthenticatedService::new(service, self.clone())
    }
}

#[derive(Clone)]
pub struct AuthenticatedService<S> {
    service: S,
    authenticator: Authenticator,
}

impl<S> AuthenticatedService<S> {
    pub fn new(service: S, authenticator: Authenticator) -> Self {
        Self { service, authenticator }
    }
}

impl<S> Service<Request<BoxBody>> for AuthenticatedService<S>
where
    S: Service<Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<Status>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut service = self.service.clone();
        Box::pin(async move {
            let req = spawn_blocking(move || authenticator.authenticate(req)).await.unwrap()?;
            service.call(req).await
        })
    }
}

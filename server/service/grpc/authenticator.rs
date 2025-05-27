/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use diagnostics::metrics::ActionKind;
use futures::future::BoxFuture;
use http::Request;
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};

use crate::{
    authentication::authenticate,
    service::grpc::{
        diagnostics::run_with_diagnostics_async,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage},
    },
    state::LocalServerState,
};
use crate::state::ServerState;

#[derive(Clone)]
pub struct Authenticator {
    server_state: Arc<Box<dyn ServerState + Send + Sync>>,
}

impl Authenticator {
    pub(crate) fn new(server_state: Arc<Box<dyn ServerState + Send + Sync>>) -> Self {
        Self { server_state }
    }
}

impl Authenticator {
    pub async fn authenticate(&self, request: Request<BoxBody>) -> Result<Request<BoxBody>, Status> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager(),
            None::<&str>,
            ActionKind::Authenticate,
            || async {
                authenticate(self.server_state.clone(), request)
                    .await
                    .map_err(|typedb_source| typedb_source.into_error_message().into_status())
            },
        )
        .await
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
    inner: S,
    authenticator: Authenticator,
}

impl<S> AuthenticatedService<S> {
    const AUTHENTICATION_FREE_METHODS: &'static [&'static str] = &["connection_open", "authentication_token_create"];

    pub fn new(inner: S, authenticator: Authenticator) -> Self {
        Self { inner, authenticator }
    }

    fn is_authentication_required(request: &Request<BoxBody>) -> bool {
        if let Some(method) = request.uri().path().split('/').last() {
            !Self::AUTHENTICATION_FREE_METHODS.contains(&method)
        } else {
            true
        }
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

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let request = match Self::is_authentication_required(&request) {
                true => authenticator.authenticate(request).await?,
                false => request,
            };
            inner.call(request).await
        })
    }
}

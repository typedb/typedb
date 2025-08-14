/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{convert, sync::Arc};

use axum::{body::Body, response::IntoResponse};
use diagnostics::metrics::ActionKind;
use futures::future::BoxFuture;
use http::{Request, Response};
use tower::{Layer, Service};

use crate::{
    authentication::authenticate,
    service::http::{diagnostics::run_with_diagnostics_async, error::HttpServiceError},
    state::ArcServerState,
};

#[derive(Clone, Debug)]
pub struct Authenticator {
    server_state: ArcServerState,
}

impl Authenticator {
    pub(crate) fn new(server_state: ArcServerState) -> Self {
        Self { server_state }
    }
}

impl Authenticator {
    pub async fn authenticate(&self, request: Request<Body>) -> Result<Request<Body>, impl IntoResponse> {
        run_with_diagnostics_async(
            self.server_state.diagnostics_manager().await,
            None::<&str>,
            ActionKind::Authenticate,
            || async {
                authenticate(self.server_state.clone(), request)
                    .await
                    .map_err(|typedb_source| HttpServiceError::Authentication { typedb_source })
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
    pub fn new(inner: S, authenticator: Authenticator) -> Self {
        Self { inner, authenticator }
    }
}

impl<S> Service<Request<Body>> for AuthenticatedService<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = convert::Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            match authenticator.authenticate(request).await {
                Ok(req) => inner.call(req).await,
                Err(err) => Ok(err.into_response()),
            }
        })
    }
}

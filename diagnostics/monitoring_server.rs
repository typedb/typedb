/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use hyper::{
    header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use tokio::task;

use crate::Diagnostics;

#[derive(Debug)]
pub struct MonitoringServer {
    diagnostics: Arc<Diagnostics>,
    port: u16,
}

impl MonitoringServer {
    pub fn new(diagnostics: Arc<Diagnostics>, port: u16) -> Self {
        Self { diagnostics, port }
    }

    pub async fn start_serving(&self) {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let diagnostics = self.diagnostics.clone();

        task::spawn(async move {
            let make_svc = make_service_fn(move |_| {
                let diagnostics = diagnostics.clone();
                async move {
                    Ok::<_, hyper::Error>(service_fn(move |req| {
                        let diagnostics = diagnostics.clone();
                        async move { MonitoringServer::handle_request(req, diagnostics).await }
                    }))
                }
            });

            match Server::try_bind(&addr) {
                Ok(server) => {
                    if let Err(e) = server.serve(make_svc).await {
                        eprintln!("WARNING: Diagnostics monitoring server error: '{}'", e);
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: Diagnostics monitoring server could not get initialised on {}: '{}'", addr, e)
                }
            }
        });
    }

    async fn handle_request(req: Request<Body>, diagnostics: Arc<Diagnostics>) -> Result<Response<Body>, Infallible> {
        if req.uri().path() != "/diagnostics" {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "text/plain")
                .header(CONNECTION, "close")
                .body(Body::from("Not Found"))
                .unwrap());
        }

        let query = req.uri().query().unwrap_or("").to_lowercase();

        let (content_type, body) = if query.contains("format=json") {
            ("application/json", diagnostics.to_monitoring_json().to_string().into_bytes())
        } else {
            ("text/plain", diagnostics.to_prometheus_data().into_bytes())
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, content_type)
            .header(CONTENT_LENGTH, body.len().to_string())
            .header(CONNECTION, "close")
            .body(Body::from(body))
            .unwrap())
    }
}

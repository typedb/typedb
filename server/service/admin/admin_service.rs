/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use resource::constants::server::DEFAULT_USER_NAME;
use system::concepts::Credential;
use tonic::{Request, Response, Status};

use crate::{admin_proto, authentication::Accessor, service::grpc::IntoGrpcStatus, state::ServerState};

#[derive(Debug, Clone)]
pub struct AdminService {
    server_state: Arc<ServerState>,
}

impl AdminService {
    pub fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }

    pub fn server_state(&self) -> &Arc<ServerState> {
        &self.server_state
    }
}

#[tonic::async_trait]
impl admin_proto::type_db_admin_server::TypeDbAdmin for AdminService {
    async fn server_version(
        &self,
        _request: Request<admin_proto::server_version::Req>,
    ) -> Result<Response<admin_proto::server_version::Res>, Status> {
        let distribution_info = self.server_state.distribution_info();
        Ok(Response::new(admin_proto::server_version::Res {
            distribution: distribution_info.distribution.to_string(),
            version: distribution_info.version.to_string(),
        }))
    }

    async fn server_status(
        &self,
        _request: Request<admin_proto::server_status::Req>,
    ) -> Result<Response<admin_proto::server_status::Res>, Status> {
        let status = self.server_state.servers().status().await.map_err(IntoGrpcStatus::into_status)?;
        let grpc = admin_proto::EndpointStatus {
            listen_address: status.grpc_listen_address().unwrap_or_default().to_string(),
            advertise_address: status.grpc_advertise_address().map(str::to_string),
        };
        let http = status.http_listen_address().map(|listen| admin_proto::EndpointStatus {
            listen_address: listen.to_string(),
            advertise_address: status.http_advertise_address().map(str::to_string),
        });
        let admin_address = status.admin_address().map(|a| a.to_string());
        let monitoring_address = status.monitoring_address().map(|a| a.to_string());
        Ok(Response::new(admin_proto::server_status::Res { grpc: Some(grpc), http, admin_address, monitoring_address }))
    }

    async fn user_reset_password(
        &self,
        request: Request<admin_proto::user_reset_password::Req>,
    ) -> Result<Response<admin_proto::user_reset_password::Res>, Status> {
        let admin_proto::user_reset_password::Req { username, password } = request.into_inner();
        if username.is_empty() {
            return Err(Status::invalid_argument("Username must not be empty"));
        }
        if password.is_empty() {
            return Err(Status::invalid_argument("Password must not be empty"));
        }

        let credential = Credential::new_password(&password);
        let accessor = Accessor(DEFAULT_USER_NAME.to_string());
        self.server_state
            .users()
            .update(accessor, &username, None, Some(credential))
            .await
            .map_err(IntoGrpcStatus::into_status)?;

        Ok(Response::new(admin_proto::user_reset_password::Res {}))
    }
}

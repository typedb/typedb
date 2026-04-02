/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{admin_proto, state::ServerState};

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
}

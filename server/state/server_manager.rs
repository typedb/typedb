/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::{
    error::{ArcServerStateError, LocalServerStateError},
    status::{LocalServerStatus, ServerStatus},
};

use super::BoxServerStatus;

#[async_trait]
pub trait ServerManager: Debug + Send + Sync {
    async fn server_status(&self) -> Result<BoxServerStatus, ArcServerStateError>;

    async fn servers_all(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError>;

    async fn servers_register(
        &self,
        clustering_id: u64,
        clustering_address: String,
    ) -> Result<(), ArcServerStateError>;

    async fn servers_deregister(&self, clustering_id: u64) -> Result<(), ArcServerStateError>;
}

#[derive(Debug)]
pub struct LocalServerManager {
    server_status: LocalServerStatus,
}

impl LocalServerManager {
    pub fn new(server_status: LocalServerStatus) -> Self {
        Self { server_status }
    }
}

#[async_trait]
impl ServerManager for LocalServerManager {
    async fn server_status(&self) -> Result<BoxServerStatus, ArcServerStateError> {
        Ok(Box::new(self.server_status.clone()))
    }

    async fn servers_all(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError> {
        self.server_status().await.map(|status| vec![status])
    }

    async fn servers_register(
        &self,
        _clustering_id: u64,
        _clustering_address: String,
    ) -> Result<(), ArcServerStateError> {
        Err(Arc::new(LocalServerStateError::NotSupportedByDistribution {
            description: "exclusive to TypeDB Cloud and TypeDB Enterprise".to_string(),
        }))
    }

    async fn servers_deregister(&self, _clustering_id: u64) -> Result<(), ArcServerStateError> {
        Err(Arc::new(LocalServerStateError::NotSupportedByDistribution {
            description: "exclusive to TypeDB Cloud and TypeDB Enterprise".to_string(),
        }))
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Debug;

use async_trait::async_trait;

use super::BoxServerStatus;
use crate::{
    error::ArcServerStateError,
    status::LocalServerStatus,
};

#[async_trait]
pub trait ServerOperator: Debug + Send + Sync {
    async fn status(&self) -> Result<BoxServerStatus, ArcServerStateError>;
    async fn statuses(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError>;
}

#[derive(Debug)]
pub struct LocalServerOperator {
    server_status: LocalServerStatus,
}

impl LocalServerOperator {
    pub fn new(server_status: LocalServerStatus) -> Self {
        Self { server_status }
    }
}

#[async_trait]
impl ServerOperator for LocalServerOperator {
    async fn status(&self) -> Result<BoxServerStatus, ArcServerStateError> {
        Ok(Box::new(self.server_status.clone()))
    }
    
    async fn statuses(&self) -> Result<Vec<BoxServerStatus>, ArcServerStateError> {
        self.status().await.map(|status| vec![status])
    }
}

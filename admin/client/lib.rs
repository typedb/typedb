/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod command;
pub mod commands;
pub mod error;
pub mod repl;

use std::sync::Arc;

use server_admin_proto::type_db_admin_client::TypeDbAdminClient;
use tonic::transport::Channel;

use crate::error::AdminError;

pub type AdminClient = TypeDbAdminClient<Channel>;

pub async fn connect(address: &str) -> Result<AdminClient, AdminError> {
    let channel = connect_channel(address).await?;
    Ok(TypeDbAdminClient::new(channel))
}

pub async fn connect_channel(address: &str) -> Result<Channel, AdminError> {
    tonic::transport::Endpoint::from_shared(format_insecure_address(address))
        .map_err(|source| AdminError::ConnectionFailed {
            address: address.to_string(),
            source: Arc::new(source.into()),
        })?
        .connect()
        .await
        .map_err(|source| AdminError::ConnectionFailed { address: address.to_string(), source: Arc::new(source) })
}

fn format_insecure_address(address: &str) -> String {
    format!("http://{address}")
}

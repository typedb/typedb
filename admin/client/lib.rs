/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod command;
pub mod commands;
pub mod error;
pub mod repl;
pub mod transport;

use std::path::Path;

use server_admin_proto::type_db_admin_client::TypeDbAdminClient;
use tonic::transport::Channel;

use crate::error::AdminError;
pub use crate::transport::connect_channel;

pub type AdminClient = TypeDbAdminClient<Channel>;

pub async fn connect(endpoint: &Path) -> Result<AdminClient, AdminError> {
    let channel = connect_channel(endpoint).await?;
    Ok(TypeDbAdminClient::new(channel))
}

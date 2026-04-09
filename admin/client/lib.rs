/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod command;
pub mod commands;
pub mod error;
pub mod repl;

use server_admin_proto::type_db_admin_client::TypeDbAdminClient;
use tonic::transport::Channel;

pub type AdminClient = TypeDbAdminClient<Channel>;

pub async fn connect(address: &str) -> Result<AdminClient, tonic::transport::Error> {
    TypeDbAdminClient::connect(format_insecure_address(address)).await
}

pub async fn connect_channel(address: &str) -> Result<Channel, tonic::transport::Error> {
    tonic::transport::Endpoint::from_shared(format_insecure_address(address))?.connect().await
}

fn format_insecure_address(address: &str) -> String {
    format!("http://{address}")
}

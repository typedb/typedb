/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod cli;
pub mod commands;

use server::admin_proto::type_db_admin_client::TypeDbAdminClient;
use tonic::transport::Channel;

pub type AdminClient = TypeDbAdminClient<Channel>;

pub async fn connect(address: &str) -> Result<AdminClient, tonic::transport::Error> {
    let endpoint = format!("http://{address}");
    TypeDbAdminClient::connect(endpoint).await
}

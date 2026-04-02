/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Subcommand;
use server::admin_proto;

use crate::AdminClient;

#[derive(Subcommand)]
pub enum ServerCommand {
    Version,
}

pub async fn execute(client: &mut AdminClient, command: ServerCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ServerCommand::Version => {
            let response = client.server_version(admin_proto::server_version::Req {}).await?;
            let res = response.into_inner();
            println!("{} {}", res.distribution, res.version);
            Ok(())
        }
    }
}

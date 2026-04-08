/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use server_admin_proto as admin_proto;

use crate::{
    command::{CommandDefinition, CommandRegistry, CommandResult},
    AdminClient,
};

pub fn register(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(CommandDefinition {
            tokens: &["server", "version"],
            description: "Show server version",
            args: &[],
            executor: |client, _args| Box::pin(server_version(client)),
        })
        .register(CommandDefinition {
            tokens: &["server", "status"],
            description: "Show server endpoint addresses",
            args: &[],
            executor: |client, _args| Box::pin(server_status(client)),
        })
}

async fn server_version(client: &mut AdminClient) -> CommandResult {
    let response = client.server_version(admin_proto::server_version::Req {}).await?;
    let res = response.into_inner();
    println!("{} {}", res.distribution, res.version);
    Ok(())
}

async fn server_status(client: &mut AdminClient) -> CommandResult {
    let response = client.server_status(admin_proto::server_status::Req {}).await?;
    let res = response.into_inner();

    if let Some(grpc) = &res.grpc {
        print!("gRPC:  {}", grpc.serving_address);
        if grpc.connection_address != grpc.serving_address {
            print!(" (connect via {})", grpc.connection_address);
        }
        println!();
    }

    if let Some(http) = &res.http {
        print!("HTTP:  {}", http.serving_address);
        if http.connection_address != http.serving_address {
            print!(" (connect via {})", http.connection_address);
        }
        println!();
    }

    if let Some(admin_address) = &res.admin_address {
        println!("Admin: {admin_address}");
    }

    Ok(())
}

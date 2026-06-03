/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::server_info::{EndpointInfo, ServingInfo, print_serving_block};
use server_admin_proto as admin_proto;

use crate::{
    AdminClient,
    command::{CommandDefinition, CommandRegistry, CommandResult, Result},
};

pub fn register(registry: CommandRegistry) -> CommandRegistry {
    registry
        .register(CommandDefinition {
            tokens: &["server", "version"],
            description: "Show server version",
            args: &[],
            executor: |ctx| Box::pin(server_version(ctx.client)),
        })
        .register(CommandDefinition {
            tokens: &["server", "status"],
            description: "Show server endpoint addresses",
            args: &[],
            executor: |ctx| Box::pin(server_status(ctx.client)),
        })
}

pub async fn execute_server_version(client: &mut AdminClient) -> Result<server_admin_proto::server_version::Res> {
    let response = client.server_version(admin_proto::server_version::Req {}).await?;
    Ok(response.into_inner())
}

pub async fn execute_server_status(client: &mut AdminClient) -> Result<server_admin_proto::server_status::Res> {
    let response = client.server_status(admin_proto::server_status::Req {}).await?;
    Ok(response.into_inner())
}

async fn server_version(client: &mut AdminClient) -> CommandResult {
    let res = execute_server_version(client).await?;
    println!("{} {}", res.distribution, res.version);
    Ok(())
}

async fn server_status(client: &mut AdminClient) -> CommandResult {
    let res = execute_server_status(client).await?;
    let info = ServingInfo {
        grpc: res.grpc.as_ref().map(endpoint_from_proto).unwrap_or_default(),
        http: res.http.as_ref().map(endpoint_from_proto),
        admin: res.admin_address,
        monitoring: res.monitoring_address,
    };
    println!("Status: running");
    print_serving_block(&info);
    Ok(())
}

fn endpoint_from_proto(e: &admin_proto::EndpointStatus) -> resource::server_info::EndpointInfo {
    EndpointInfo { listen: Some(e.listen_address.clone()), advertise: e.advertise_address.clone() }
}

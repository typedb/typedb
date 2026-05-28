/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use server_admin_proto as admin_proto;

use crate::{
    AdminClient,
    command::{CommandDefinition, CommandRegistry, CommandResult},
    error::AdminError,
};

pub fn register(registry: CommandRegistry) -> CommandRegistry {
    registry.register(CommandDefinition {
        tokens: &["user", "reset-password"],
        description: "Reset a user's password. Reads the new password from stdin (one line).",
        args: &["username"],
        executor: |ctx| {
            let args = ctx.args.to_vec();
            Box::pin(async move { reset_password(ctx.client, &args).await })
        },
    })
}

async fn reset_password(client: &mut AdminClient, args: &[String]) -> CommandResult {
    let username = match args {
        [username] => username.clone(),
        _ => {
            return Err(AdminError::InvalidArgCount { usage: "user reset-password <username>".to_string() });
        }
    };

    let password = read_password()?;

    client
        .users_reset_password(admin_proto::users_reset_password::Req { username: username.clone(), password })
        .await?;
    println!("Password updated for user '{username}'.");
    Ok(())
}

fn read_password() -> Result<String, AdminError> {
    let password = rpassword::prompt_password("New password: ").map_err(|err| AdminError::InvalidArgument {
        name: "password".to_string(),
        reason: format!("could not read password: {err}"),
    })?;
    if password.is_empty() {
        return Err(AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    Ok(password)
}

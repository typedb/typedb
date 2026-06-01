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
        description: "Reset a user's password. Prompts for the new password if not supplied.",
        args: &["username", "[new-password]"],
        executor: |ctx| {
            let args = ctx.args.to_vec();
            Box::pin(async move { reset_password(ctx.client, &args).await })
        },
    })
}

async fn reset_password(client: &mut AdminClient, args: &[String]) -> CommandResult {
    let (username, password) = match args {
        [username] => (username.clone(), prompt_password()?),
        [username, password] => (username.clone(), password.clone()),
        _ => {
            return Err(AdminError::InvalidArgCount {
                usage: "user reset-password <username> [<new-password>]".to_string(),
            });
        }
    };

    if password.is_empty() {
        return Err(AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: "must not be empty".to_string(),
        });
    }

    client
        .users_reset_password(admin_proto::users_reset_password::Req { username: username.clone(), password })
        .await?;
    println!("Password updated for user '{username}'.");
    Ok(())
}

fn prompt_password() -> Result<String, AdminError> {
    use std::io::{BufRead, IsTerminal};
    if std::io::stdin().is_terminal() {
        rpassword::prompt_password("New password: ").map_err(|err| AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: format!("could not read password: {err}"),
        })
    } else {
        let mut buf = String::new();
        std::io::stdin().lock().read_line(&mut buf).map_err(|err| AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: format!("could not read password from stdin: {err}"),
        })?;
        Ok(buf.trim_end_matches(['\r', '\n']).to_string())
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{self, BufRead, IsTerminal};

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
            Box::pin(async move { set_password(ctx.client, &args).await })
        },
    })
}

async fn set_password(client: &mut AdminClient, args: &[String]) -> CommandResult {
    let username = match args {
        [username] => username.clone(),
        _ => {
            return Err(AdminError::InvalidArgCount { usage: "user reset-password <username>".to_string() });
        }
    };

    let password = read_password_from_stdin()?;

    client.users_set_password(admin_proto::users_set_password::Req { username: username.clone(), password }).await?;
    println!("Password updated for user '{username}'.");
    Ok(())
}

fn read_password_from_stdin() -> Result<String, AdminError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: "stdin must not be a terminal; pipe the password in, e.g. `cat pw.txt | typedb-admin --command 'user reset-password <name>'`"
                .to_string(),
        });
    }
    let mut line = String::new();
    stdin.lock().read_line(&mut line).map_err(|err| AdminError::InvalidArgument {
        name: "password".to_string(),
        reason: format!("could not read from stdin: {err}"),
    })?;
    // Trim only the trailing newline (a password may legitimately contain leading
    // whitespace or end with spaces before the newline that the user wants preserved).
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    if line.is_empty() {
        return Err(AdminError::InvalidArgument {
            name: "password".to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    Ok(line)
}

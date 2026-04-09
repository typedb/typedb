/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use rustyline::{error::ReadlineError, history::FileHistory, Config, Editor};

use crate::{
    command::{CommandContext, CommandRegistry},
    commands::server::{execute_server_status, execute_server_version},
    error::AdminError,
    AdminClient,
};

const PROMPT: &str = "admin> ";
const HISTORY_FILE: &str = ".typedb_admin_history";

fn history_path() -> PathBuf {
    home::home_dir().unwrap_or_else(std::env::temp_dir).join(HISTORY_FILE)
}

pub async fn print_server_info(client: &mut AdminClient) {
    let server_version = match execute_server_version(client).await {
        Ok(version) => version,
        Err(err) => {
            eprintln!("WARNING: could not retrieve server version: {err:?}");
            return;
        }
    };
    let server_status = match execute_server_status(client).await {
        Ok(status) => status,
        Err(err) => {
            eprintln!("WARNING: could not retrieve server status: {err:?}");
            return;
        }
    };
    let admin_address = server_status.admin_address.unwrap_or_default();
    println!("Connected to {} {} ({}).", server_version.distribution, server_version.version, admin_address);
}

pub async fn run_interactive(client: &mut AdminClient, address: &str, registry: &CommandRegistry) {
    println!("Type 'help' for available commands, 'exit' to quit.\n");

    let config = Config::builder().auto_add_history(true).build();
    let mut editor: Editor<(), FileHistory> = Editor::with_history(config, FileHistory::new()).unwrap();
    let _ = editor.load_history(&history_path());

    loop {
        match editor.readline(PROMPT) {
            Ok(input) => {
                let input = input.trim();
                if input.is_empty() {
                    continue;
                }
                if let Err(err) = execute_input(client, address, registry, input).await {
                    eprintln!("[Error] {err:?}");
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error reading input: {err}");
                break;
            }
        }
    }

    let _ = editor.save_history(&history_path());
}

pub async fn run_commands(
    client: &mut AdminClient,
    address: &str,
    registry: &CommandRegistry,
    commands: &[String],
) -> i32 {
    for command in commands {
        if let Err(err) = execute_input(client, address, registry, command.trim()).await {
            eprintln!("[Error] {err:?}");
            return 1;
        }
    }
    0
}

pub async fn run_script(
    client: &mut AdminClient,
    address: &str,
    registry: &CommandRegistry,
    path: &str,
) -> Result<(), AdminError> {
    let content = std::fs::read_to_string(path).map_err(|source| AdminError::ScriptReadFailed {
        path: path.to_string(),
        source: std::sync::Arc::new(source),
    })?;
    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Err(err) = execute_input(client, address, registry, line).await {
            eprintln!("[Error] Line {}: {err:?}", line_num + 1);
            return Err(err);
        }
    }
    Ok(())
}

async fn execute_input(
    client: &mut AdminClient,
    address: &str,
    registry: &CommandRegistry,
    input: &str,
) -> Result<(), AdminError> {
    match input {
        "exit" | "quit" => std::process::exit(0),
        "help" => {
            println!("Available commands:\n{}\n  help\n  exit", registry.help_text());
            Ok(())
        }
        _ => {
            let tokens: Vec<&str> = input.split_whitespace().collect();
            match registry.find(&tokens) {
                Some((cmd, args)) => {
                    let ctx = CommandContext { client, address, args: &args };
                    (cmd.executor)(ctx).await
                }
                None => Err(AdminError::UnknownCommand { input: input.to_string() }),
            }
        }
    }
}

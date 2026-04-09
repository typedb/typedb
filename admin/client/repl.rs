/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use rustyline::{error::ReadlineError, history::FileHistory, Config, Editor};

use crate::{
    command::CommandRegistry,
    commands::server::{execute_server_status, execute_server_version},
    AdminClient,
};

const PROMPT: &str = "admin> ";
const HISTORY_FILE: &str = ".typedb_admin_history";

fn history_path() -> PathBuf {
    home::home_dir().unwrap_or_else(std::env::temp_dir).join(HISTORY_FILE)
}

pub async fn print_server_info(client: &mut AdminClient) {
    let server_version = match execute_server_version(client).await {
        Ok(server_version) => server_version,
        Err(err) => {
            eprintln!("WARNING: could not retrieve server version: {}", format_error(&err.into()));
            return;
        }
    };
    let server_status = match execute_server_status(client).await {
        Ok(server_status) => server_status,
        Err(err) => {
            eprintln!("WARNING: could not retrieve server status: {}", format_error(&err.into()));
            return;
        }
    };
    let server_admin_address = match server_status.admin_address {
        Some(admin_address) => admin_address,
        None => {
            eprintln!("WARNING: could not retrieve server admin address");
            return;
        }
    };

    println!("Connected to {} {} ({}).", server_version.distribution, server_version.version, server_admin_address);
}

pub async fn run_interactive(client: &mut AdminClient, registry: &CommandRegistry) {
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
                if let Err(err) = execute_input(client, registry, input).await {
                    eprintln!("Error: {err}");
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

pub async fn run_commands(client: &mut AdminClient, registry: &CommandRegistry, commands: &[String]) -> i32 {
    for command in commands {
        if let Err(err) = execute_input(client, registry, command.trim()).await {
            eprintln!("Error: {err}");
            return 1;
        }
    }
    0
}

pub async fn run_script(
    client: &mut AdminClient,
    registry: &CommandRegistry,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Err(err) = execute_input(client, registry, line).await {
            eprintln!("Error at line {}: {err}", line_num + 1);
            return Err(err);
        }
    }
    Ok(())
}

async fn execute_input(
    client: &mut AdminClient,
    registry: &CommandRegistry,
    input: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match input {
        "exit" | "quit" => std::process::exit(0),
        "help" => {
            println!("Available commands:\n{}\n  help\n  exit", registry.help_text());
            Ok(())
        }
        _ => {
            let tokens: Vec<&str> = input.split_whitespace().collect();
            match registry.find(&tokens) {
                Some((cmd, args)) => (cmd.executor)(client, &args).await.map_err(|err| format_error(&err).into()),
                None => Err(format!("Unknown command: {input}. Type 'help' for available commands.").into()),
            }
        }
    }
}

pub fn format_error(err: &Box<dyn std::error::Error>) -> String {
    let err_str = err.to_string();
    // tonic::Status Display format: "status: <Code>, message: "<msg>", details: [...], metadata: {...}"
    if err_str.starts_with("status: ") {
        if let Some(details_start) = err_str.find(", details:") {
            let relevant = &err_str["status: ".len()..details_start];
            if let Some(msg_start) = relevant.find(", message: \"") {
                let code = &relevant[..msg_start];
                let message = relevant[msg_start + ", message: \"".len()..].trim_end_matches('"');
                if message.is_empty() {
                    return code.to_string();
                }
                return format!("{code}: {message}");
            }
            return relevant.to_string();
        }
    }
    err_str
}

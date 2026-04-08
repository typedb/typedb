/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{self, BufRead, Write};

use crate::{command::CommandRegistry, AdminClient};

pub async fn run_interactive(client: &mut AdminClient, registry: &CommandRegistry) {
    println!("Type 'help' for available commands, 'exit' to quit.\n");
    let stdin = io::stdin();
    loop {
        print!("admin> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match stdin.lock().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(err) => {
                eprintln!("Error reading input: {err}");
                break;
            }
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        if let Err(err) = execute_input(client, registry, input).await {
            eprintln!("Error: {err}");
        }
    }
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
                Some((cmd, args)) => (cmd.executor)(client, &args).await,
                None => Err(format!("Unknown command: {input}. Type 'help' for available commands.").into()),
            }
        }
    }
}

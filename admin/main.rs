/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "typedb-admin", about = "TypeDB administration tool (local-only)")]
struct Args {
    /// Path to the local admin endpoint: a Unix socket file on Unix, a Named Pipe name
    /// (e.g. \\.\pipe\typedb_admin) on Windows. Must match the configured endpoint on
    /// the running server.
    #[arg(long)]
    socket_path: PathBuf,

    /// Execute a command and exit (repeatable)
    #[arg(short, long)]
    command: Vec<String>,

    /// Execute commands from a script file
    #[arg(long)]
    script: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let registry = typedb_admin::commands::base_commands();

    let mut client = match typedb_admin::connect(&args.socket_path).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("{err:?}");
            std::process::exit(1);
        }
    };

    if let Some(script) = &args.script {
        if !args.command.is_empty() {
            eprintln!("Cannot specify both --command and --script");
            std::process::exit(1);
        }
        if let Err(err) = typedb_admin::repl::run_script(&mut client, &registry, script).await {
            eprintln!("{err:?}");
            std::process::exit(1);
        }
    } else if !args.command.is_empty() {
        let code = typedb_admin::repl::run_commands(&mut client, &registry, &args.command).await;
        std::process::exit(code);
    } else {
        typedb_admin::repl::print_server_info(&mut client).await;
        typedb_admin::repl::run_interactive(&mut client, &registry).await;
    }
}

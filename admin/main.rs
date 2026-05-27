/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use clap::Parser;

/// Environment variable consulted when `--socket-path` is not supplied. Lets scripts
/// avoid hard-coding the per-deployment socket location.
const SOCKET_PATH_ENV: &str = "TYPEDB_ADMIN_SOCKET";

#[derive(Parser)]
#[command(name = "typedb-admin", about = "TypeDB administration tool (local Unix socket)")]
struct Args {
    /// Path to the TypeDB admin Unix domain socket. Defaults to the value of
    /// $TYPEDB_ADMIN_SOCKET when set.
    #[arg(long)]
    socket_path: Option<PathBuf>,

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

    let socket_path = match args.socket_path.or_else(|| std::env::var_os(SOCKET_PATH_ENV).map(PathBuf::from)) {
        Some(path) => path,
        None => {
            eprintln!("error: --socket-path is required (or set ${SOCKET_PATH_ENV})");
            std::process::exit(2);
        }
    };

    let mut client = match typedb_admin::connect(&socket_path).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("{err:?}");
            std::process::exit(1);
        }
    };

    let address_display = socket_path.to_string_lossy().into_owned();
    if let Some(script) = &args.script {
        if !args.command.is_empty() {
            eprintln!("Cannot specify both --command and --script");
            std::process::exit(1);
        }
        if let Err(err) = typedb_admin::repl::run_script(&mut client, &address_display, &registry, script).await {
            eprintln!("{err:?}");
            std::process::exit(1);
        }
    } else if !args.command.is_empty() {
        let code = typedb_admin::repl::run_commands(&mut client, &address_display, &registry, &args.command).await;
        std::process::exit(code);
    } else {
        typedb_admin::repl::print_server_info(&mut client).await;
        typedb_admin::repl::run_interactive(&mut client, &address_display, &registry).await;
    }
}

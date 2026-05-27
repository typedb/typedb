/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use clap::Parser;
use resource::constants::server::ADMIN_DEFAULT_ADDRESS;

/// Environment variable consulted when `--token-path` is not supplied. Lets scripts avoid
/// hard-coding the per-deployment token-file location.
const TOKEN_PATH_ENV: &str = "TYPEDB_ADMIN_TOKEN_PATH";

#[derive(Parser)]
#[command(name = "typedb-admin", about = "TypeDB administration tool")]
struct Args {
    /// Server admin endpoint address
    #[arg(long, default_value = ADMIN_DEFAULT_ADDRESS)]
    address: String,

    /// Path to the admin bearer-token file. Defaults to the value of
    /// $TYPEDB_ADMIN_TOKEN_PATH when set.
    #[arg(long)]
    token_path: Option<PathBuf>,

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

    let token_path = match args.token_path.or_else(|| std::env::var_os(TOKEN_PATH_ENV).map(PathBuf::from)) {
        Some(path) => path,
        None => {
            eprintln!("error: --token-path is required (or set ${TOKEN_PATH_ENV})");
            std::process::exit(2);
        }
    };

    let address = &args.address;
    let mut client = match typedb_admin::connect(address, &token_path).await {
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
        if let Err(err) = typedb_admin::repl::run_script(&mut client, address, &registry, script).await {
            eprintln!("{err:?}");
            std::process::exit(1);
        }
    } else if !args.command.is_empty() {
        let code = typedb_admin::repl::run_commands(&mut client, address, &registry, &args.command).await;
        std::process::exit(code);
    } else {
        typedb_admin::repl::print_server_info(&mut client).await;
        typedb_admin::repl::run_interactive(&mut client, address, &registry).await;
    }
}

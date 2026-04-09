/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use resource::constants::server::ADMIN_DEFAULT_ADDRESS;

#[derive(Parser)]
#[command(name = "typedb-admin", about = "TypeDB administration tool")]
struct Args {
    /// Server admin endpoint address
    #[arg(long, default_value = ADMIN_DEFAULT_ADDRESS)]
    address: String,

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

    let address = &args.address;
    let mut client = match typedb_admin::connect(address).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("Failed to connect to {address}: {err}");
            std::process::exit(1);
        }
    };

    if let Some(script) = &args.script {
        if !args.command.is_empty() {
            eprintln!("Cannot specify both --command and --script");
            std::process::exit(1);
        }
        if let Err(err) = typedb_admin::repl::run_script(&mut client, address, &registry, script).await {
            eprintln!("{err}");
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

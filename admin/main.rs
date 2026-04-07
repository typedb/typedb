/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use typedb_admin::cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut client = typedb_admin::connect(&cli.address).await?;
    match cli.command {
        Command::Server { command } => typedb_admin::commands::server::execute(&mut client, command).await,
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::{Parser, Subcommand};

use crate::commands::server::ServerCommand;

#[derive(Parser)]
#[command(name = "typedb-admin", about = "TypeDB admin CLI")]
pub struct Cli {
    #[arg(long, default_value = "127.0.0.1:1728")]
    pub address: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
}

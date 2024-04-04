/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::collections::HashMap;

use cucumber::{StatsWriter, World};
use database::Database;
use durability::wal::WAL;
use server::typedb;
use test_utils::TempDir;

mod connection;
mod util;

#[derive(Debug, Default, World)]
pub struct Context {
    server_dir: Option<TempDir>,
    server: Option<typedb::Server>,
}

impl Context {
    pub async fn test(glob: &'static str) -> bool {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        !Self::cucumber()
            .repeat_failed()
            .fail_on_skipped()
            .with_default_cli()
            .after(|_, _, _, _, context| {
                Box::pin(async {
                    context.unwrap().after_scenario().await.unwrap();
                })
            })
            .filter_run(glob, |_, _, sc| !sc.tags.iter().any(|tag| is_ignore(tag)))
            .await
            .execution_has_failed()
    }

    async fn after_scenario(&mut self) -> Result<(), ()> {
        Ok(())
    }

    pub fn server(&self) -> Option<&typedb::Server> {
        self.server.as_ref()
    }

    pub fn server_mut(&mut self) -> Option<&mut typedb::Server> {
        self.server.as_mut()
    }

    pub fn databases(&self) -> &HashMap<String, Database<WAL>> {
        self.server().unwrap().databases()
    }
}

fn is_ignore(tag: &str) -> bool {
    tag == "ignore" || tag == "ignore-typedb"
}

#[macro_export]
macro_rules! generic_step {
    {$(#[step($pattern:expr)])+ $vis:vis $async:ident fn $fn_name:ident $args:tt $(-> $res:ty)? $body:block} => {
        $(
        #[::cucumber::given($pattern)]
        #[::cucumber::when($pattern)]
        #[::cucumber::then($pattern)]
        )+
        $vis $async fn $fn_name $args $(-> $res)? $body
    };
}

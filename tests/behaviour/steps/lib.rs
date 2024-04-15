/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::collections::HashMap;
use std::sync::Arc;

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

    pub fn databases(&self) -> &HashMap<String, Arc<Database<WAL>>> {
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

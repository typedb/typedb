/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{collections::HashMap, sync::Arc};

use ::concept::thing::{attribute::Attribute, object::Object};
use cucumber::{StatsWriter, World};
use database::Database;
use server::typedb;
use storage::durability_client::WALClient;
use test_utils::TempDir;

mod assert;
mod concept;
mod connection;
mod params;
mod transaction_context;
mod util;

use thing_util::ObjectWithKey;
use transaction_context::ActiveTransaction;

mod thing_util {
    use concept::thing::{attribute::Attribute, object::Object};

    #[derive(Debug)]
    pub struct ObjectWithKey {
        pub object: Object<'static>,
        pub key: Option<Attribute<'static>>,
    }

    impl ObjectWithKey {
        pub fn new_with_key(object: Object<'static>, key: Attribute<'static>) -> Self {
            Self { object, key: Some(key) }
        }

        pub fn new(object: Object<'static>) -> Self {
            Self { object, key: None }
        }
    }
}

#[derive(Debug, Default, World)]
pub struct Context {
    server_dir: Option<TempDir>,
    server: Option<typedb::Server>,
    active_transaction: Option<ActiveTransaction>,

    objects: HashMap<String, Option<ObjectWithKey>>,
    object_lists: HashMap<String, Vec<Object<'static>>>,
    attributes: HashMap<String, Option<Attribute<'static>>>,
    attribute_lists: HashMap<String, Vec<Attribute<'static>>>,
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
            .filter_run(glob, |_, _, sc| {
                sc.name.contains(std::env::var("SCENARIO_FILTER").as_deref().unwrap_or(""))
                    && !sc.tags.iter().any(|tag| is_ignore(tag))
            })
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

    pub fn databases(&self) -> &HashMap<String, Arc<Database<WALClient>>> {
        self.server().unwrap().databases()
    }

    pub fn set_transaction(&mut self, txn: ActiveTransaction) {
        self.active_transaction = Some(txn);
    }

    pub fn transaction(&mut self) -> Option<&mut ActiveTransaction> {
        self.active_transaction.as_mut()
    }

    pub fn take_transaction(&mut self) -> Option<ActiveTransaction> {
        self.active_transaction.take()
    }
}

fn is_ignore(tag: &str) -> bool {
    tag == "ignore" || tag == "ignore-typedb"
}

#[macro_export]
macro_rules! generic_step {
    {$(#[step($pattern:expr)])+ $vis:vis $async:ident fn $fn_name:ident $args:tt $(-> $res:ty)? $body:block} => {
        #[allow(unused)]
        $vis $async fn $fn_name $args $(-> $res)? $body

        const _: () = {
            $(
            #[::cucumber::given($pattern)]
            #[::cucumber::when($pattern)]
            #[::cucumber::then($pattern)]
            )+
            $vis $async fn step $args $(-> $res)? $body
        };
    };
}

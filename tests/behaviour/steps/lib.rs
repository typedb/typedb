/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashMap, HashSet},
    iter, mem,
    path::Path,
    sync::{Arc, Mutex},
};

use ::concept::thing::{attribute::Attribute, object::Object};
use cucumber::{gherkin::Feature, StatsWriter, World};
use database::{Database, DatabaseDeleteError};
use futures::{
    future::Either,
    stream::{self, StreamExt},
};
use itertools::Itertools;
use server::typedb;
use storage::durability_client::WALClient;
use thing_util::ObjectWithKey;
use transaction_context::ActiveTransaction;

mod assert;
mod concept;
mod connection;
mod params;
mod query;
mod transaction_context;
mod util;

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

#[derive(Debug, Default)]
struct SingletonParser {
    basic: cucumber::parser::Basic,
}

impl<I: AsRef<Path>> cucumber::Parser<I> for SingletonParser {
    type Cli = <cucumber::parser::Basic as cucumber::Parser<I>>::Cli;
    type Output = stream::FlatMap<
        stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
        Either<
            stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
            stream::Iter<iter::Once<Result<Feature, cucumber::parser::Error>>>,
        >,
        fn(
            Result<Feature, cucumber::parser::Error>,
        ) -> Either<
            stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
            stream::Iter<iter::Once<Result<Feature, cucumber::parser::Error>>>,
        >,
    >;

    fn parse(self, input: I, cli: Self::Cli) -> Self::Output {
        self.basic.parse(input, cli).flat_map(|res| match res {
            Ok(mut feature) => {
                let scenarios = mem::take(&mut feature.scenarios);
                let singleton_features = scenarios
                    .into_iter()
                    .map(|scenario| {
                        Ok(Feature {
                            name: feature.name.clone() + " :: " + &scenario.name,
                            scenarios: vec![scenario],
                            ..feature.clone()
                        })
                    })
                    .collect_vec();
                Either::Left(stream::iter(singleton_features))
            }
            Err(err) => Either::Right(stream::iter(iter::once(Err(err)))),
        })
    }
}

#[derive(Debug, Default, World)]
pub struct Context {
    server: Option<Arc<Mutex<typedb::Server>>>,
    active_transaction: Option<ActiveTransaction>,

    objects: HashMap<String, Option<ObjectWithKey>>,
    object_lists: HashMap<String, Vec<Object<'static>>>,
    attributes: HashMap<String, Option<Attribute<'static>>>,
    attribute_lists: HashMap<String, Vec<Attribute<'static>>>,
}

impl Context {
    pub async fn test<I: AsRef<Path>>(glob: I, clean_databases_after: bool) -> bool {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        !Self::cucumber::<I>()
            .with_parser(SingletonParser::default())
            .repeat_failed()
            .fail_on_skipped()
            .with_default_cli()
            .after(move |_, _, _, _, context| {
                Box::pin(async move {
                    context.unwrap().after_scenario(clean_databases_after).await.unwrap();
                })
            })
            .filter_run(glob, |_, _, sc| {
                sc.name.contains(std::env::var("SCENARIO_FILTER").as_deref().unwrap_or(""))
                    && !sc.tags.iter().any(|tag| is_ignore(tag))
            })
            .await
            .execution_has_failed()
    }

    async fn after_scenario(&mut self, clean_databases: bool) -> Result<(), ()> {
        if self.active_transaction.is_some() {
            self.close_transaction()
        }

        if clean_databases {
            let database_names = self.server().unwrap().lock().unwrap().database_manager().database_names();
            for database_name in database_names {
                self.server().unwrap().lock().unwrap().database_manager().delete_database(&database_name).unwrap();
            }
        }

        Ok(())
    }

    pub fn close_transaction(&mut self) {
        match self.take_transaction().unwrap() {
            ActiveTransaction::Read(tx) => tx.close(),
            ActiveTransaction::Write(tx) => tx.close(),
            ActiveTransaction::Schema(tx) => tx.close(),
        }
    }

    pub fn server(&self) -> Option<&Mutex<typedb::Server>> {
        self.server.as_deref()
    }

    pub fn database(&self, name: &str) -> Arc<Database<WALClient>> {
        self.server().unwrap().lock().unwrap().database_manager().database(name).unwrap()
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

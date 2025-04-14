/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::HashMap,
    error::Error,
    fmt, iter, mem,
    path::Path,
    sync::{Arc, Mutex},
};

use ::concept::thing::{attribute::Attribute, object::Object};
use ::query::error::QueryError;
use cucumber::{gherkin::Feature, StatsWriter, World};
use database::Database;
use futures::{
    future::Either,
    stream::{self, StreamExt},
};
use itertools::Itertools;
use server::server::Server;
use storage::durability_client::WALClient;
use thing_util::ObjectWithKey;
use transaction_context::ActiveTransaction;

use crate::query_answer_context::QueryAnswer;

mod concept;
mod connection;
mod json;
mod query;
mod query_answer_context;
mod transaction_context;
pub(crate) mod util;

mod thing_util {
    use concept::thing::{attribute::Attribute, object::Object};

    #[derive(Debug)]
    pub struct ObjectWithKey {
        pub object: Object,
        pub key: Option<Attribute>,
    }

    impl ObjectWithKey {
        pub fn new_with_key(object: Object, key: Attribute) -> Self {
            Self { object, key: Some(key) }
        }

        pub fn new(object: Object) -> Self {
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
    server: Option<Arc<Mutex<Server>>>,
    active_transaction: Option<ActiveTransaction>,
    active_concurrent_transactions: Vec<ActiveTransaction>,

    query_answer: Option<QueryAnswer>,
    objects: HashMap<String, Option<ObjectWithKey>>,
    object_lists: HashMap<String, Vec<Object>>,
    attributes: HashMap<String, Option<Attribute>>,
    attribute_lists: HashMap<String, Vec<Attribute>>,
}

impl Context {
    pub async fn test<I: AsRef<Path>>(glob: I, clean_databases_after: bool) -> bool {
        logger::initialise_logging_global();
        !Self::cucumber::<I>()
            .with_parser(SingletonParser::default())
            .repeat_failed()
            .fail_on_skipped()
            .with_default_cli()
            .before(move |_, _, _, _| {
                // cucumber removes the default hook before each scenario and restores it after!
                std::panic::set_hook(Box::new(move |info| println!("{}", info)));
                Box::pin(async move {})
            })
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

    fn close_active_concurrent_transactions(&mut self) {
        while let Some(tx) = self.active_concurrent_transactions.pop() {
            Self::close_transaction(tx);
        }
    }

    async fn after_scenario(&mut self, clean_databases: bool) -> Result<(), ()> {
        if self.active_transaction.is_some() {
            self.close_active_transaction();
        }
        self.close_active_concurrent_transactions();

        if clean_databases {
            let database_names = self.server().unwrap().lock().unwrap().database_manager().database_names();
            for database_name in database_names {
                self.server().unwrap().lock().unwrap().database_manager().delete_database(&database_name).unwrap();
            }
        }

        Ok(())
    }

    pub fn close_active_transaction(&mut self) {
        Self::close_transaction(
            self.take_transaction().expect("Expected a transaction to close. No active transaction is found"),
        )
    }

    pub fn close_transaction(tx: ActiveTransaction) {
        match tx {
            ActiveTransaction::Read(tx) => tx.close(),
            ActiveTransaction::Write(tx) => tx.close(),
            ActiveTransaction::Schema(tx) => tx.close(),
        }
    }

    pub fn server(&self) -> Option<&Mutex<Server>> {
        self.server.as_deref()
    }

    pub async fn database(&self, name: &str) -> Arc<Database<WALClient>> {
        self.server().unwrap().lock().unwrap().database_manager().database(name).unwrap()
    }

    pub fn set_transaction(&mut self, tx: ActiveTransaction) {
        self.active_transaction = Some(tx);
    }

    pub fn set_concurrent_transactions(&mut self, txs: Vec<ActiveTransaction>) {
        self.close_active_concurrent_transactions();
        self.active_concurrent_transactions = txs;
    }

    pub fn get_concurrent_transactions(&self) -> &[ActiveTransaction] {
        &self.active_concurrent_transactions
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

#[derive(Debug, Clone)]
pub enum BehaviourTestExecutionError {
    UseInvalidTransactionAsWrite,
    UseInvalidTransactionAsSchema,
    Query(QueryError),
}

impl fmt::Display for BehaviourTestExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Error for BehaviourTestExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UseInvalidTransactionAsWrite => None,
            Self::UseInvalidTransactionAsSchema => None,
            Self::Query(_) => None,
        }
    }
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

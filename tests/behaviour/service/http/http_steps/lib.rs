/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    fmt::Formatter,
    iter, mem,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use cucumber::{gherkin::Feature, StatsWriter, World};
use error::typedb_error;
use futures::{
    future::Either,
    stream::{self, StreamExt},
};
use hyper::{client::HttpConnector, http, Client, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use itertools::Itertools;
use options::TransactionOptions;
use server::{
    error::ServerOpenError,
    service::{
        http::message::{query::QueryAnswerResponse, transaction::TransactionResponse},
        AnswerType, QueryType,
    },
};
use test_utils::TempDir;
use tokio::{
    task::JoinHandle,
    time::{sleep, Duration},
};

use crate::{
    connection::start_typedb,
    message::{check_health, databases, databases_delete, transactions_close, users, users_delete, users_update},
};

macro_rules! in_background {
    ($context:ident, |$background:ident| $expr:expr) => {
        let mut $background = crate::HttpContext { http_client: crate::create_http_client(), auth_token: None };
        let response = crate::message::authenticate_default(&$background).await;
        $background.auth_token = Some(response.token);
        $expr
    };
}
pub(crate) use in_background;
use server::service::http::message::transaction::TransactionOptionsPayload;

mod connection;
mod message;
mod params;
mod query;
mod util;

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

#[derive(Debug, Clone)]
pub struct HttpContext {
    pub http_client: Client<HttpConnector>,
    pub auth_token: Option<String>,
}

#[derive(World)]
pub struct Context {
    pub tls_root_ca: PathBuf,
    pub transaction_options: Option<TransactionOptionsPayload>,
    pub http_context: HttpContext,
    pub transaction_ids: VecDeque<String>,
    pub background_transaction_ids: VecDeque<String>,
    pub answer: Option<QueryAnswerResponse>,
    pub concurrent_answers: Vec<QueryAnswerResponse>,
    pub concurrent_answers_last_consumed_index: usize,
    pub shutdown_sender: Option<tokio::sync::watch::Sender<()>>,
    pub handler: Option<(TempDir, JoinHandle<Result<(), ServerOpenError>>)>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("tls_root_ca", &self.tls_root_ca)
            .field("transaction_options", &self.transaction_options)
            .field("transaction_ids", &self.transaction_ids)
            .field("background_transaction_ids", &self.background_transaction_ids)
            .field("answer", &self.answer)
            .field("concurrent_answers", &self.concurrent_answers)
            .finish()
    }
}

impl Context {
    const DEFAULT_ADDRESS: &'static str = "127.0.0.1:8000";
    const HTTP_PROTOCOL: &'static str = "http";
    const HTTPS_PROTOCOL: &'static str = "https";
    const DEFAULT_API_VERSION: &'static str = "v1";
    const DEFAULT_DATABASE: &'static str = "test";
    const ADMIN_USERNAME: &'static str = "admin";
    const ADMIN_PASSWORD: &'static str = "password";

    const SERVER_START_CHECK_INTERVAL: Duration = Duration::from_secs(1);
    const SERVER_MAX_START_TIME: Duration = Duration::from_secs(10);

    pub async fn test<I: AsRef<Path>>(glob: I) -> bool {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        let (shutdown_sender, server) = start_typedb().await;
        Self::wait_server_start().await;

        let result = !Self::cucumber::<I>()
            .repeat_failed()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .with_parser(SingletonParser::default())
            .with_default_cli()
            .before(move |_, _, _, _| {
                // cucumber removes the default hook before each scenario and restores it after!
                std::panic::set_hook(Box::new(move |info| println!("{}", info)));
                Box::pin(async move {})
            })
            .after(|_, _, _, _, context| {
                Box::pin(async {
                    context.unwrap().after_scenario().await;
                })
            })
            .filter_run(glob, |_, _, sc| {
                sc.name.contains(std::env::var("SCENARIO_FILTER").as_deref().unwrap_or(""))
                    && !sc.tags.iter().any(Self::is_ignore_tag)
            })
            .await
            .execution_has_failed();

        shutdown_sender.send(()).expect("Expected shutdown signal to be sent from tests");
        server.join().expect("Expected server's join").expect("Expected server's successful stop");
        result
    }

    async fn wait_server_start() {
        let starting_since = Instant::now();
        let http_client = create_http_client();
        loop {
            let result = check_health(http_client.clone()).await;
            if result.is_ok() {
                break;
            }
            if Instant::now().duration_since(starting_since) > Self::SERVER_MAX_START_TIME {
                panic!("Server has not started in {:?}. Aborting tests!", Self::SERVER_MAX_START_TIME);
            }
            tokio::time::sleep(Self::SERVER_START_CHECK_INTERVAL).await;
        }
    }

    pub fn default_versioned_endpoint() -> String {
        Self::versioned_endpoint(Self::HTTP_PROTOCOL, Self::DEFAULT_ADDRESS, Self::DEFAULT_API_VERSION)
    }

    pub fn versioned_endpoint(protocol: &str, address: &str, api_version: &str) -> String {
        format!("{}://{}/{}", protocol, address, api_version)
    }

    fn is_ignore_tag(t: &String) -> bool {
        t == "ignore" || t == "ignore-http"
    }

    pub async fn after_scenario(&mut self) {
        self.cleanup_transactions().await;
        self.cleanup_background_transactions().await;
        self.cleanup_databases().await;
        self.cleanup_users().await;
        self.cleanup_answers().await;
        self.cleanup_concurrent_answers().await;
    }

    pub async fn cleanup_databases(&mut self) {
        in_background!(context, |background| {
            for database in databases(&background).await.unwrap().databases {
                databases_delete(&background, &database.name).await.unwrap();
            }
        });
    }

    pub async fn cleanup_transactions(&mut self) {
        while let Some(transaction_id) = self.try_take_transaction() {
            transactions_close(&self.http_context, &transaction_id).await.unwrap();
        }
    }

    pub async fn cleanup_background_transactions(&mut self) {
        while let Some(transaction_id) = self.try_take_background_transaction() {
            transactions_close(&self.http_context, &transaction_id).await.unwrap();
        }
    }

    pub async fn cleanup_users(&mut self) {
        in_background!(context, |background| {
            for user in users(&background).await.unwrap().users {
                if user.username != Context::ADMIN_USERNAME {
                    users_delete(&background, &user.username).await.unwrap();
                }
            }
            users_update(&background, Context::ADMIN_USERNAME, Context::ADMIN_PASSWORD).await.unwrap();
        });
    }

    pub async fn cleanup_answers(&mut self) {
        self.answer = None;
    }

    pub async fn cleanup_concurrent_answers(&mut self) {
        self.concurrent_answers = Vec::new();
        self.concurrent_answers_last_consumed_index = 0;
    }

    pub fn transaction_opt(&self) -> Option<&String> {
        self.transaction_ids.get(0)
    }

    pub fn transaction(&self) -> &String {
        self.transaction_ids.get(0).unwrap()
    }

    pub fn take_transaction(&mut self) -> String {
        self.transaction_ids.pop_front().unwrap()
    }

    pub fn try_take_transaction(&mut self) -> Option<String> {
        self.transaction_ids.pop_front()
    }

    pub fn try_take_background_transaction(&mut self) -> Option<String> {
        self.background_transaction_ids.pop_front()
    }

    pub fn push_transaction(
        &mut self,
        transaction: Result<TransactionResponse, HttpBehaviourTestError>,
    ) -> Result<(), HttpBehaviourTestError> {
        self.transaction_ids.push_back(transaction?.transaction_id.to_string());
        Ok(())
    }

    pub fn push_background_transaction(
        &mut self,
        transaction: Result<TransactionResponse, HttpBehaviourTestError>,
    ) -> Result<(), HttpBehaviourTestError> {
        self.background_transaction_ids.push_back(transaction?.transaction_id.to_string());
        Ok(())
    }

    pub async fn set_transactions(&mut self, transactions: VecDeque<String>) {
        self.cleanup_transactions().await;
        self.transaction_ids = transactions;
    }

    pub fn set_answer(
        &mut self,
        answer: Result<QueryAnswerResponse, HttpBehaviourTestError>,
    ) -> Result<(), HttpBehaviourTestError> {
        let answer = answer?;
        self.answer = Some(answer);
        Ok(())
    }

    pub fn set_concurrent_answers(&mut self, answers: Vec<QueryAnswerResponse>) {
        self.concurrent_answers = answers;
        self.concurrent_answers_last_consumed_index = 0;
    }

    pub fn get_answer(&self) -> Option<&QueryAnswerResponse> {
        self.answer.as_ref()
    }

    pub fn get_answer_query_type(&self) -> Option<QueryType> {
        self.answer.as_ref().map(|answer| answer.query_type)
    }

    pub fn get_answer_type(&self) -> Option<AnswerType> {
        self.answer.as_ref().map(|answer| answer.answer_type)
    }

    pub fn get_answer_row_index(&mut self, index: usize) -> &serde_json::Value {
        self.answer.as_ref().unwrap().answers.as_ref().unwrap().get(index).unwrap()
    }

    pub fn get_concurrent_answers_index(&mut self) -> usize {
        self.concurrent_answers_last_consumed_index
    }

    pub fn set_concurrent_answers_index(&mut self, index: usize) {
        self.concurrent_answers_last_consumed_index = index;
    }

    pub fn get_concurrent_answers(&self) -> &Vec<QueryAnswerResponse> {
        &self.concurrent_answers
    }

    pub fn init_transaction_options_if_needed(&mut self) {
        if self.transaction_options.is_none() {
            self.transaction_options = Some(TransactionOptionsPayload::default());
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        let tls_root_ca = match std::env::var("ROOT_CA") {
            Ok(root_ca) => PathBuf::from(root_ca),
            Err(_) => PathBuf::new(),
        };
        Self {
            tls_root_ca,
            transaction_options: None,
            http_context: HttpContext { http_client: create_http_client(), auth_token: None },
            transaction_ids: VecDeque::new(),
            background_transaction_ids: VecDeque::new(),
            answer: None,
            concurrent_answers: Vec::new(),
            concurrent_answers_last_consumed_index: 0,
            shutdown_sender: None,
            handler: None,
        }
    }
}

fn create_http_client() -> Client<HttpConnector> {
    Client::builder().build::<_, hyper::Body>(HttpConnector::new())
}

fn create_https_client() -> Client<HttpsConnector<HttpConnector>> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots() // TODO: Use custom roots?
        .expect("Expected native roots")
        .https_only()
        .enable_http1()
        .build();
    Client::builder().build::<_, hyper::Body>(https)
}

typedb_error! {
    pub HttpBehaviourTestError(component = "HTTP Behaviour Test", prefix = "HTB") {
        InvalidConceptConversion(1, "Invalid concept conversion."),
        InvalidValueRetrieval(2, "Could not retrieve a '{type_}' value.", type_: String),
        HttpError(3, "Http error.", source: Arc<http::Error>),
        HyperError(4, "Hyper error.", source: Arc<hyper::Error>),
        StatusError(5, "Status Error {code}: {message}", code: StatusCode, message: String),
        UnavailableRowVariable(6, "Cannot get concept from a concept row by variable '{variable}'.", variable: String),
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

#[macro_export]
macro_rules! assert_err {
    ($expr:expr) => {{
        let res = $expr;
        assert!(res.is_err(), "{res:?}")
    }};
}

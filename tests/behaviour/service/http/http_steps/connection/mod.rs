/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex};

use cucumber::{given, then, when};
use hyper::{
    client::HttpConnector,
    header::{AUTHORIZATION, CONTENT_TYPE},
    Body, Client, Method, Request, Uri,
};
use itertools::Either;
use macro_rules_attribute::apply;
use params::{self, check_boolean};
use serde::Deserialize;
use serde_json::json;
use server::{error::ServerOpenError, parameters::config::Config, server::Server};
use test_utils::{create_tmp_dir, TempDir};
use tokio::{
    sync::OnceCell,
    task::JoinHandle,
    time::{sleep, Instant},
};

use crate::{
    generic_step,
    message::{authenticate, authenticate_default, databases, users},
    Context, HttpBehaviourTestError, HttpContext,
};

mod database;
mod transaction;
mod user;

const GRPC_ADDRESS: &str = "0.0.0.0:1729";
const HTTP_ADDRESS: &str = "0.0.0.0:8000";
const DISTRIBUTION: &str = "TypeDB CE TEST";
const VERSION: &str = "0.0.0";

pub(crate) async fn start_typedb(
) -> (tokio::sync::watch::Sender<()>, std::thread::JoinHandle<Result<(), ServerOpenError>>) {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
    let shutdown_sender_clone = shutdown_sender.clone();
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let config = Config::new(GRPC_ADDRESS).server_http_address(HTTP_ADDRESS).development_mode(true).build();

        let server_future = async {
            let server = Server::new_with_external_shutdown(
                config,
                "logo",
                DISTRIBUTION,
                VERSION,
                None,
                shutdown_sender_clone,
                shutdown_receiver,
            )
            .await
            .expect("Failed to start TypeDB server");

            server.serve().await
        };

        rt.block_on(server_future)
    });

    (shutdown_sender, handle)
}

fn change_host(address: &str, new_host: &str) -> String {
    let parts: Vec<&str> = address.split(':').collect();
    assert_eq!(parts.len(), 2);
    format!("{}:{}", new_host, parts[1])
}

fn change_port(address: &str, new_port: &str) -> String {
    let parts: Vec<&str> = address.split(':').collect();
    assert_eq!(parts.len(), 2);
    format!("{}:{}", parts[0], new_port)
}

#[apply(generic_step)]
#[step("typedb starts")]
#[step("connection is open: true")]
#[step("connection is open: false")]
pub async fn connection_ignore(_: &mut Context) {}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
pub async fn connection_opens_with_default_authentication(context: &mut Context) {
    context.http_context.auth_token = Some(authenticate_default(&context.http_context).await.token);
}

#[apply(generic_step)]
#[step(expr = "connection opens with username '{word}', password '{word}'{may_error}")]
async fn connection_opens_with_authentication(
    context: &mut Context,
    username: String,
    password: String,
    may_error: params::MayError,
) {
    if let Either::Left(response) = may_error.check(
        authenticate(
            &context.http_context,
            Context::default_versioned_endpoint().as_str(),
            username.as_ref(),
            password.as_ref(),
        )
        .await,
    ) {
        context.http_context.auth_token = Some(response.token);
    }
}

#[apply(generic_step)]
#[step(expr = "connection opens with a wrong host{may_error}")]
async fn connection_opens_with_a_wrong_host(context: &mut Context, may_error: params::MayError) {
    // TODO: Support cluster
    may_error.check(
        authenticate(
            &context.http_context,
            Context::versioned_endpoint(
                Context::HTTP_PROTOCOL,
                &change_host(Context::DEFAULT_ADDRESS, "surely-not-localhost"),
                Context::DEFAULT_API_VERSION,
            )
            .as_str(),
            Context::ADMIN_USERNAME,
            Context::ADMIN_PASSWORD,
        )
        .await,
    );
}

#[apply(generic_step)]
#[step(expr = "connection opens with a wrong port{may_error}")]
async fn connection_opens_with_a_wrong_port(context: &mut Context, may_error: params::MayError) {
    may_error.check(
        authenticate(
            &context.http_context,
            Context::versioned_endpoint(
                Context::HTTP_PROTOCOL,
                &change_port(Context::DEFAULT_ADDRESS, "0"),
                Context::DEFAULT_API_VERSION,
            )
            .as_str(),
            Context::ADMIN_USERNAME,
            Context::ADMIN_PASSWORD,
        )
        .await,
    );
}

#[apply(generic_step)]
#[step(expr = r"connection has {int} database(s)")]
async fn connection_has_count_databases(context: &mut Context, count: usize) {
    assert_eq!(databases(&context.http_context).await.expect("Expected databases").databases.len(), count);
}

#[apply(generic_step)]
#[step(expr = r"connection has {int} user(s)")]
async fn connection_has_count_users(context: &mut Context, count: usize) {
    assert_eq!(users(&context.http_context).await.expect("Expected users").users.len(), count);
}

#[apply(generic_step)]
#[step(expr = r"connection closes{may_error}")]
async fn connection_closes(context: &mut Context, may_error: params::MayError) {
    context.cleanup_transactions().await;
}

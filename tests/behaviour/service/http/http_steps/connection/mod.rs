/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{collections::HashMap, path::PathBuf};

use itertools::Either;
use macro_rules_attribute::apply;
use params::check_boolean;
use resource::server_info::ServerInfo;
use server::{
    error::ServerOpenError,
    parameters::config::{AuthenticationConfig, ConfigBuilder},
    ServerBuilder,
};
use test_utils::create_tmp_dir;

use crate::{
    generic_step,
    message::{authenticate, authenticate_default, check_health, databases, send_get_request, users},
    Context, HttpBehaviourTestError, TEST_TOKEN_EXPIRATION,
};

mod database;
mod transaction;
mod user;

const GRPC_ADDRESS: &str = "0.0.0.0:1729";
const HTTP_ADDRESS: &str = "0.0.0.0:8000";
const SERVER_INFO: ServerInfo = ServerInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0" };

fn config_path() -> PathBuf {
    return std::env::current_dir().unwrap().join("server/config.yml");
}

pub(crate) async fn start_typedb(
) -> (tokio::sync::watch::Sender<()>, std::thread::JoinHandle<Result<(), ServerOpenError>>) {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
    let shutdown_sender_clone = shutdown_sender.clone();
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let server_dir = create_tmp_dir();
        let config = ConfigBuilder::from_file(config_path())
            .expect("Failed to load config file")
            .server_address(GRPC_ADDRESS)
            .server_http_address(HTTP_ADDRESS)
            .data_directory(server_dir.as_ref())
            .development_mode(true)
            .authentication(AuthenticationConfig { token_expiration: TEST_TOKEN_EXPIRATION })
            .build()
            .unwrap();

        let server_future = async {
            let server = ServerBuilder::default()
                .server_info(SERVER_INFO)
                .shutdown_channel((shutdown_sender_clone, shutdown_receiver))
                .build(config)
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
#[step(expr = "connection is healthy: {boolean}")]
async fn connection_is_healthy(context: &mut Context, is_healthy: params::Boolean) {
    check_boolean!(is_healthy, check_health(context.http_client(), context.auth_token()).await.is_ok());
}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
pub async fn connection_opens_with_default_authentication(context: &mut Context) {
    context.http_context.auth_token = Some(authenticate_default(context.http_client()).await.token);
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
            context.http_client(),
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
            context.http_client(),
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
            context.http_client(),
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
    assert_eq!(
        databases(context.http_client(), context.auth_token()).await.expect("Expected databases").databases.len(),
        count
    );
}

#[apply(generic_step)]
#[step(expr = r"connection has {int} user(s)")]
async fn connection_has_count_users(context: &mut Context, count: usize) {
    assert_eq!(users(context.http_client(), context.auth_token()).await.expect("Expected users").users.len(), count);
}

#[apply(generic_step)]
#[step(expr = r"connection closes{may_error}")]
async fn connection_closes(context: &mut Context, may_error: params::MayError) {
    context.cleanup_transactions().await;
    context.http_context.auth_token = None;
    may_error.check(Ok::<(), HttpBehaviourTestError>(()));
}

#[apply(generic_step)]
#[step(expr = r"get endpoint\({word}\) contains field: {word}")]
async fn get_endpoint_contains_field(context: &mut Context, endpoint: String, field: String) {
    let url = format!("{}{endpoint}", Context::default_non_versioned_endpoint());
    let response = send_get_request(&context.http_context.http_client, context.auth_token(), &url, None)
        .await
        .expect("Expected GET response");
    let json_map: HashMap<String, String> = serde_json::from_str(&response).expect("Expected a json body");
    assert!(json_map.contains_key(&field));
}

#[apply(generic_step)]
#[step(expr = r"get endpoint\({word}\) redirects to: {word}")]
async fn get_endpoint_redirects_to(context: &mut Context, endpoint: String, redirect_endpoint: String) {
    let url = format!("{}{endpoint}", Context::default_non_versioned_endpoint());
    let response = send_get_request(&context.http_context.http_client, context.auth_token(), &url, None)
        .await
        .expect("Expected GET response");
    assert_eq!(response, redirect_endpoint);
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use hyper::{
    client::HttpConnector,
    header::{AUTHORIZATION, CONTENT_TYPE},
    Body, Client, Method, Request, Uri,
};
use serde_json::json;
use server::service::http::message::{
    authentication::TokenResponse,
    database::{DatabaseResponse, DatabasesResponse},
    query::{
        concept::{
            AttributeResponse, AttributeTypeResponse, EntityResponse, EntityTypeResponse, RelationResponse,
            RelationTypeResponse, RoleTypeResponse, ValueResponse,
        },
        QueryAnswerResponse, QueryOptionsPayload,
    },
    transaction::{TransactionOptionsPayload, TransactionResponse},
    user::{UserResponse, UsersResponse},
};
use url::form_urlencoded;

use crate::{Context, HttpBehaviourTestError};

async fn send_request(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    method: Method,
    url: &str,
    body: Option<&str>,
) -> Result<String, HttpBehaviourTestError> {
    let uri: Uri = url.parse().expect("Invalid URI");
    let mut req = Request::builder().method(method).uri(uri);

    if let Some(token) = auth_token.as_ref() {
        req = req.header(AUTHORIZATION, format!("Bearer {}", token.as_ref()));
    }

    let req = req
        .header(CONTENT_TYPE, "application/json")
        .body(match body {
            Some(b) => Body::from(b.to_string()),
            None => Body::empty(),
        })
        .map_err(|source| HttpBehaviourTestError::HttpError { source: Arc::new(source) })?;

    let res = http_client
        .request(req)
        .await
        .map_err(|source| HttpBehaviourTestError::HyperError { source: Arc::new(source) })?;

    let status = res.status();

    if status.is_success() {
        let body_bytes = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|source| HttpBehaviourTestError::HyperError { source: Arc::new(source) })?;
        Ok(String::from_utf8_lossy(&body_bytes).to_string())
    } else if status.is_redirection() {
        let (header, _) = res.into_parts();
        Ok(header.headers.get("location").expect("Expected redirection location").to_str().unwrap().to_string())
    } else {
        let body_bytes = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|source| HttpBehaviourTestError::HyperError { source: Arc::new(source) })?;

        let body_str = String::from_utf8_lossy(&body_bytes).to_string();
        Err(HttpBehaviourTestError::StatusError { code: status, message: body_str })
    }
}

pub async fn send_get_request(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    url: &str,
    body: Option<&str>,
) -> Result<String, HttpBehaviourTestError> {
    send_request(http_client, auth_token, Method::GET, url, body).await
}

pub async fn check_health(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
) -> Result<String, HttpBehaviourTestError> {
    let uri: Uri = format!("{}/health", Context::default_versioned_endpoint()).parse().expect("Invalid URI");
    let mut req = Request::builder().method(Method::GET).uri(uri);

    if let Some(token) = auth_token {
        req = req.header(AUTHORIZATION, format!("Bearer {}", token.as_ref()));
    }
    let req =
        req.body(Body::empty()).map_err(|source| HttpBehaviourTestError::HttpError { source: Arc::new(source) })?;

    let res = http_client
        .request(req)
        .await
        .map_err(|source| HttpBehaviourTestError::HyperError { source: Arc::new(source) })?;
    let body_bytes = hyper::body::to_bytes(res.into_body())
        .await
        .map_err(|source| HttpBehaviourTestError::HyperError { source: Arc::new(source) })?;
    Ok(String::from_utf8_lossy(&body_bytes).to_string())
}

pub async fn authenticate_default(http_client: &Client<HttpConnector>) -> TokenResponse {
    authenticate(
        http_client,
        Context::default_versioned_endpoint().as_str(),
        Context::ADMIN_USERNAME,
        Context::ADMIN_PASSWORD,
    )
    .await
    .expect("Expected default auth")
}

pub async fn authenticate(
    http_client: &Client<HttpConnector>,
    endpoint: &str,
    username: &str,
    password: &str,
) -> Result<TokenResponse, HttpBehaviourTestError> {
    let url = format!("{}/signin", endpoint);
    let json_body = json!({
        "username": username,
        "password": password,
    });
    let response =
        send_request(http_client, None::<&str>, Method::POST, &url, Some(json_body.to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn databases(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
) -> Result<DatabasesResponse, HttpBehaviourTestError> {
    let url = format!("{}/databases", Context::default_versioned_endpoint());
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn databases_get(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
) -> Result<DatabaseResponse, HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn databases_create(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(http_client, auth_token, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn databases_delete(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(http_client, auth_token, Method::DELETE, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn databases_schema(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
) -> Result<String, HttpBehaviourTestError> {
    let url =
        format!("{}/databases/{}/schema", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(response)
}

pub async fn databases_type_schema(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
) -> Result<String, HttpBehaviourTestError> {
    let url =
        format!("{}/databases/{}/type-schema", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(response)
}

pub async fn users(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
) -> Result<UsersResponse, HttpBehaviourTestError> {
    let url = format!("{}/users", Context::default_versioned_endpoint());
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn users_get(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    username: &str,
) -> Result<UserResponse, HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let response = send_request(http_client, auth_token, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn users_create(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    username: &str,
    password: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let json_body = json!({
        "password": password,
    });
    let response =
        send_request(http_client, auth_token, Method::POST, &url, Some(json_body.to_string().as_str())).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn users_update(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    username: &str,
    password: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let json_body = json!({
        "password": password,
    });
    let response =
        send_request(http_client, auth_token, Method::PUT, &url, Some(json_body.to_string().as_str())).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn users_delete(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    username: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let response = send_request(http_client, auth_token, Method::DELETE, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_open(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
    transaction_type: &str,
    transaction_options: &Option<TransactionOptionsPayload>,
) -> Result<TransactionResponse, HttpBehaviourTestError> {
    let url = format!("{}/transactions/open", Context::default_versioned_endpoint());
    let mut json_map = serde_json::Map::new();
    json_map.insert("databaseName".to_string(), json!(database_name));
    json_map.insert("transactionType".to_string(), json!(transaction_type));
    if let Some(transaction_options) = transaction_options {
        json_map.insert("transactionOptions".to_string(), json!(transaction_options));
    }

    let response =
        send_request(http_client, auth_token, Method::POST, &url, Some(json!(json_map).to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn transactions_close(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    transaction_id: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/close", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(http_client, auth_token, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_commit(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    transaction_id: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/commit", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(http_client, auth_token, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_rollback(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    transaction_id: &str,
) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/rollback", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(http_client, auth_token, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_query(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    transaction_id: &str,
    query_options: &Option<QueryOptionsPayload>,
    query: &str,
) -> Result<QueryAnswerResponse, HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/query", Context::default_versioned_endpoint(), transaction_id);
    let mut json_map = serde_json::Map::new();
    json_map.insert("query".to_string(), json!(query));
    if let Some(query_options) = query_options {
        json_map.insert("queryOptions".to_string(), json!(query_options));
    }
    let response =
        send_request(http_client, auth_token, Method::POST, &url, Some(json!(json_map).to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn query(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    database_name: &str,
    transaction_type: &str,
    query: &str,
    transaction_options: &Option<TransactionOptionsPayload>,
    query_options: &Option<QueryOptionsPayload>,
    commit: bool,
) -> Result<QueryAnswerResponse, HttpBehaviourTestError> {
    let url = format!("{}/query", Context::default_versioned_endpoint());
    let mut json_map = serde_json::Map::new();
    json_map.insert("databaseName".to_string(), json!(database_name));
    json_map.insert("transactionType".to_string(), json!(transaction_type));
    json_map.insert("query".to_string(), json!(query));
    json_map.insert("commit".to_string(), json!(commit));
    if let Some(transaction_options) = transaction_options {
        json_map.insert("transactionOptions".to_string(), json!(transaction_options));
    }
    if let Some(query_options) = query_options {
        json_map.insert("queryOptions".to_string(), json!(query_options));
    }
    // TODO: Add other params?
    let response =
        send_request(http_client, auth_token, Method::POST, &url, Some(json!(json_map).to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

fn encode_path_variable(var: &str) -> String {
    form_urlencoded::byte_serialize(var.as_bytes()).collect()
}

#[derive(Debug)]
pub enum ConceptResponse {
    EntityType(EntityTypeResponse),
    RelationType(RelationTypeResponse),
    AttributeType(AttributeTypeResponse),
    RoleType(RoleTypeResponse),
    Entity(EntityResponse),
    Relation(RelationResponse),
    Attribute(AttributeResponse),
    Value(ValueResponse),
}

impl ConceptResponse {
    pub fn is_type(&self) -> bool {
        match self {
            ConceptResponse::EntityType(_)
            | ConceptResponse::RelationType(_)
            | ConceptResponse::AttributeType(_)
            | ConceptResponse::RoleType(_) => true,
            ConceptResponse::Entity(_)
            | ConceptResponse::Relation(_)
            | ConceptResponse::Attribute(_)
            | ConceptResponse::Value(_) => false,
        }
    }

    pub fn is_instance(&self) -> bool {
        match self {
            ConceptResponse::EntityType(_)
            | ConceptResponse::RelationType(_)
            | ConceptResponse::AttributeType(_)
            | ConceptResponse::RoleType(_)
            | ConceptResponse::Value(_) => false,
            ConceptResponse::Entity(_) | ConceptResponse::Relation(_) | ConceptResponse::Attribute(_) => true,
        }
    }

    pub fn get_label(&self) -> &str {
        self.try_get_label().unwrap_or("none")
    }

    pub fn try_get_label(&self) -> Option<&str> {
        match self {
            ConceptResponse::EntityType(entity_type) => Some(entity_type.label.as_str()),
            ConceptResponse::RelationType(relation_type) => Some(relation_type.label.as_str()),
            ConceptResponse::AttributeType(attribute_type) => Some(attribute_type.label.as_str()),
            ConceptResponse::RoleType(role_type) => Some(role_type.label.as_str()),
            ConceptResponse::Entity(entity) => entity.type_.as_ref().map(|val| val.label.as_str()),
            ConceptResponse::Relation(relation) => relation.type_.as_ref().map(|val| val.label.as_str()),
            ConceptResponse::Attribute(attribute) => attribute.type_.as_ref().map(|val| val.label.as_str()),
            ConceptResponse::Value(value) => Some(value.value_type.as_ref()),
        }
    }

    pub fn get_value_type(&self) -> Option<&str> {
        match self {
            ConceptResponse::AttributeType(attribute_type) => {
                attribute_type.value_type.as_ref().map(|val| val.as_str())
            }
            ConceptResponse::Attribute(attribute) => Some(&attribute.value_type),
            ConceptResponse::Value(value) => Some(&value.value_type),
            other => panic!("Kind '{other:?}' does not value types"),
        }
    }

    pub fn try_get_value_type(&self) -> Option<&str> {
        match self {
            ConceptResponse::AttributeType(attribute_type) => {
                attribute_type.value_type.as_ref().map(|val| val.as_str())
            }
            ConceptResponse::Attribute(attribute) => Some(&attribute.value_type),
            ConceptResponse::Value(value) => Some(&value.value_type),
            _ => None,
        }
    }

    pub fn get_value_type_or_none(&self) -> &str {
        self.try_get_value_type().unwrap_or_else(|| "none")
    }

    pub fn get_value(&self) -> &serde_json::Value {
        match self {
            ConceptResponse::Attribute(attribute) => &attribute.value,
            ConceptResponse::Value(value) => &value.value,
            other => panic!("Kind '{other:?}' does not have values"),
        }
    }

    pub fn try_get_value(&self) -> Option<&serde_json::Value> {
        match self {
            ConceptResponse::Attribute(attribute) => Some(&attribute.value),
            ConceptResponse::Value(value) => Some(&value.value),
            other => None,
        }
    }

    pub fn try_get_iid(&self) -> Option<&String> {
        match self {
            // TODO: Maybe add attributes
            ConceptResponse::Entity(entity) => Some(&entity.iid),
            ConceptResponse::Relation(relation) => Some(&relation.iid),
            other => None,
        }
    }
}

impl From<serde_json::Value> for ConceptResponse {
    fn from(value: serde_json::Value) -> Self {
        match value.get("kind").expect("Expected kind in the answer").as_str().expect("Expected str") {
            "entityType" => ConceptResponse::EntityType(serde_json::from_value(value).expect("Invalid EntityType")),
            "relationType" => {
                ConceptResponse::RelationType(serde_json::from_value(value).expect("Invalid RelationType"))
            }
            "attributeType" => {
                ConceptResponse::AttributeType(serde_json::from_value(value).expect("Invalid AttributeType"))
            }
            "roleType" => ConceptResponse::RoleType(serde_json::from_value(value).expect("Invalid RoleType")),
            "entity" => ConceptResponse::Entity(serde_json::from_value(value).expect("Invalid Entity")),
            "relation" => ConceptResponse::Relation(serde_json::from_value(value).expect("Invalid Relation")),
            "attribute" => ConceptResponse::Attribute(serde_json::from_value(value).expect("Invalid Attribute")),
            "value" => ConceptResponse::Value(serde_json::from_value(value).expect("Invalid Value")),
            other => panic!("Cannot convert kind '{other:?}' to a ConceptResponse"),
        }
    }
}

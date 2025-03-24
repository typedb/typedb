/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{delete, get, post, put},
    RequestPartsExt, Router,
};
use concurrency::TokioIntervalRunner;
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use http::StatusCode;
use itertools::Itertools;
use options::{QueryOptions, TransactionOptions};
use resource::constants::common::SECONDS_IN_MINUTE;
use system::concepts::{Credential, User};
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        oneshot, RwLock,
    },
    time::timeout,
};
use tower_http::cors::CorsLayer;
use user::{permission_manager::PermissionManager, user_manager::UserManager};
use uuid::Uuid;

use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor},
    service::{
        http::{
            diagnostics::{run_with_diagnostics, run_with_diagnostics_async},
            error::HttpServiceError,
            message::{
                authentication::{encode_token, SigninPayload},
                body::{JsonBody, PlainTextBody},
                database::{encode_database, encode_databases, DatabasePath},
                query::{QueryOptionsPayload, QueryPayload, TransactionQueryPayload},
                transaction::{encode_transaction, TransactionOpenPayload, TransactionPath},
                user::{encode_user, encode_users, CreateUserPayload, UpdateUserPayload, UserPath},
                version::ProtocolVersion,
            },
            transaction_service::{
                QueryAnswer, TransactionRequest, TransactionResponder, TransactionService, TransactionServiceResponse,
            },
        },
        transaction_service::TRANSACTION_REQUEST_BUFFER_SIZE,
        QueryType,
    },
};

type TransactionRequestSender = Sender<(TransactionRequest, TransactionResponder)>;

#[derive(Debug, Clone)]
struct TransactionInfo {
    pub owner: String,
    pub database_name: String,
    pub request_sender: TransactionRequestSender,
    pub transaction_timeout_millis: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    transaction_services: Arc<RwLock<HashMap<Uuid, TransactionInfo>>>,
    shutdown_receiver: tokio::sync::watch::Receiver<()>,
    _transaction_cleanup_job: Arc<TokioIntervalRunner>,
}

impl TypeDBService {
    const TRANSACTION_CHECK_INTERVAL: Duration = Duration::from_secs(5 * SECONDS_IN_MINUTE);
    const QUERY_ENDPOINT_COMMIT_DEFAULT: bool = true;

    pub(crate) fn new(
        address: SocketAddr,
        database_manager: Arc<DatabaseManager>,
        user_manager: Arc<UserManager>,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        shutdown_receiver: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        let transaction_request_senders = Arc::new(RwLock::new(HashMap::new()));

        let controlled_transactions = transaction_request_senders.clone();
        let transaction_cleanup_job = Arc::new(TokioIntervalRunner::new_with_initial_delay(
            move || {
                let transactions = controlled_transactions.clone();
                async move {
                    Self::cleanup_closed_transactions(transactions).await;
                }
            },
            Self::TRANSACTION_CHECK_INTERVAL,
            Self::TRANSACTION_CHECK_INTERVAL,
            false,
        ));

        Self {
            address,
            database_manager,
            user_manager,
            credential_verifier,
            token_manager,
            diagnostics_manager,
            transaction_services: transaction_request_senders,
            shutdown_receiver,
            _transaction_cleanup_job: transaction_cleanup_job,
        }
    }

    async fn cleanup_closed_transactions(transactions: Arc<RwLock<HashMap<Uuid, TransactionInfo>>>) {
        let mut transactions = transactions.write().await;
        transactions.retain(|_, info| !info.request_sender.is_closed());
    }

    pub(crate) fn address(&self) -> &SocketAddr {
        &self.address
    }

    async fn transaction_new(
        service: &TypeDBService,
        owner: String,
        payload: TransactionOpenPayload,
    ) -> Result<(TransactionInfo, u64), HttpServiceError> {
        let (request_sender, request_stream) = channel(TRANSACTION_REQUEST_BUFFER_SIZE);
        let options =
            payload.transaction_options.map(|options| options.into()).unwrap_or_else(|| TransactionOptions::default());
        let transaction_timeout_millis = options.transaction_timeout_millis;
        let mut transaction_service = TransactionService::new(
            service.database_manager.clone(),
            service.diagnostics_manager.clone(),
            request_stream,
            service.shutdown_receiver.clone(),
        );

        let database_name = payload.database_name;

        let processing_time = transaction_service
            .open(payload.transaction_type, database_name.clone(), options)
            .await
            .map_err(|typedb_source| HttpServiceError::Transaction { typedb_source })?;

        tokio::spawn(async move { transaction_service.listen().await });
        Ok((TransactionInfo { owner, database_name, request_sender, transaction_timeout_millis }, processing_time))
    }

    async fn transaction_request(
        transaction: &TransactionInfo,
        request: TransactionRequest,
        error_if_closed: bool,
    ) -> Result<TransactionServiceResponse, HttpServiceError> {
        let (result_sender, result_receiver) = oneshot::channel();
        if let Err(_) = transaction.request_sender.send((request, TransactionResponder(result_sender))).await {
            return match error_if_closed {
                false => Ok(TransactionServiceResponse::Ok),
                true => Err(HttpServiceError::no_open_transaction()),
            };
        }

        match timeout(Duration::from_millis(transaction.transaction_timeout_millis), result_receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(HttpServiceError::no_open_transaction()),
            Err(_) => Err(HttpServiceError::RequestTimeout {}),
        }
    }

    fn build_query_request(query_options_payload: Option<QueryOptionsPayload>, query: String) -> TransactionRequest {
        let query_options =
            query_options_payload.map(|options| options.into()).unwrap_or_else(|| QueryOptions::default_http());
        TransactionRequest::Query(query_options, query)
    }

    fn try_get_query_response(
        transaction_response: TransactionServiceResponse,
    ) -> Result<QueryAnswer, HttpServiceError> {
        match transaction_response {
            TransactionServiceResponse::Query(query_response) => Ok(query_response),
            TransactionServiceResponse::Err(typedb_source) => Err(HttpServiceError::Transaction { typedb_source }),
            TransactionServiceResponse::Ok => {
                Err(HttpServiceError::Internal { details: "unexpected transaction response".to_string() })
            }
        }
    }
}

impl TypeDBService {
    pub(crate) fn create_protected_router<T>(service: Arc<TypeDBService>) -> Router<T> {
        Router::new()
            .route("/:version/databases", get(Self::databases))
            .route("/:version/databases/:database-name", get(Self::databases_get))
            .route("/:version/databases/:database-name", post(Self::databases_create))
            .route("/:version/databases/:database-name", delete(Self::databases_delete))
            .route("/:version/databases/:database-name/schema", get(Self::databases_schema))
            .route("/:version/databases/:database-name/type-schema", get(Self::databases_type_schema))
            .route("/:version/users", get(Self::users))
            .route("/:version/users/:username", get(Self::users_get))
            .route("/:version/users/:username", post(Self::users_create))
            .route("/:version/users/:username", put(Self::users_update))
            .route("/:version/users/:username", delete(Self::users_delete))
            .route("/:version/transactions/open", post(Self::transaction_open))
            .route("/:version/transactions/:transaction-id/commit", post(Self::transactions_commit))
            .route("/:version/transactions/:transaction-id/close", post(Self::transactions_close))
            .route("/:version/transactions/:transaction-id/rollback", post(Self::transactions_rollback))
            .route("/:version/transactions/:transaction-id/query", post(Self::transactions_query))
            .route("/:version/query", post(Self::query))
            .with_state(service)
    }

    pub(crate) fn create_unprotected_router<T>(service: Arc<TypeDBService>) -> Router<T> {
        Router::new()
            .route("/health", get(Self::health))
            .route("/:version/health", get(Self::health))
            .route("/:version/signin", post(Self::signin))
            .with_state(service)
    }

    pub(crate) fn create_cors_layer() -> CorsLayer {
        CorsLayer::permissive()
    }

    async fn health() -> impl IntoResponse {
        StatusCode::NO_CONTENT
    }

    async fn signin(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        JsonBody(payload): JsonBody<SigninPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(service.diagnostics_manager.clone(), None::<&str>, ActionKind::SignIn, || async {
            service
                .credential_verifier
                .verify_password(&payload.username, &payload.password)
                .map_err(|typedb_source| HttpServiceError::Authentication { typedb_source })?;
            Ok(JsonBody(encode_token(service.token_manager.new_token(payload.username).await)))
        })
        .await
    }

    async fn databases(_version: ProtocolVersion, State(service): State<Arc<TypeDBService>>) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
            Ok(JsonBody(encode_databases(service.database_manager.database_names())))
        })
    }

    async fn databases_get(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabasesContains,
            || {
                let database_name = service
                    .database_manager
                    .database(&database_path.database_name)
                    .ok_or(HttpServiceError::NotFound {})?
                    .name()
                    .to_string();
                Ok(JsonBody(encode_database(database_name)))
            },
        )
    }

    async fn databases_create(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabasesCreate,
            || {
                service
                    .database_manager
                    .create_database(&database_path.database_name)
                    .map_err(|typedb_source| HttpServiceError::DatabaseCreate { typedb_source })
            },
        )
    }

    async fn databases_delete(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabaseDelete,
            || {
                service
                    .database_manager
                    .delete_database(&database_path.database_name)
                    .map_err(|typedb_source| HttpServiceError::DatabaseDelete { typedb_source })
            },
        )
    }

    async fn databases_schema(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabaseSchema,
            || {
                // service
                //     .database_manager
                //     .xxxxxx(&database_path.database_name)
                //     .map_err(|typedb_source| HttpServiceError::DatabaseDelete { typedb_source })
                // Ok::<_, HttpServiceError>(PlainTextBody("".to_string())) // TODO: Return this when implemented
                Ok::<_, HttpServiceError>(StatusCode::NOT_IMPLEMENTED)
            },
        )
    }

    async fn databases_type_schema(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabaseTypeSchema,
            || {
                // service
                //     .database_manager
                //     .xxxxxx(&database_path.database_name)
                //     .map_err(|typedb_source| HttpServiceError::DatabaseDelete { typedb_source })
                // Ok::<_, HttpServiceError>(PlainTextBody("".to_string())) // TODO: Return this when implemented
                Ok::<_, HttpServiceError>(StatusCode::NOT_IMPLEMENTED)
            },
        )
    }

    async fn users(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
    ) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::UsersAll, || {
            if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
                return Err(HttpServiceError::operation_not_permitted());
            }
            Ok(JsonBody(encode_users(service.user_manager.all())))
        })
    }

    async fn users_get(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
    ) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::UsersContains, || {
            if !PermissionManager::exec_user_get_permitted(accessor.as_str(), &user_path.username) {
                return Err(HttpServiceError::operation_not_permitted());
            }
            service
                .user_manager
                .get(&user_path.username)
                .map_err(|typedb_source| HttpServiceError::UserGet { typedb_source })?
                .map(|(user, _)| JsonBody(encode_user(&user)))
                .ok_or(HttpServiceError::NotFound {})
        })
    }

    async fn users_create(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
        JsonBody(payload): JsonBody<CreateUserPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::UsersCreate, || {
            if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
                return Err(HttpServiceError::operation_not_permitted());
            }
            let user = User { name: user_path.username };
            let credential = Credential::new_password(payload.password.as_str());
            service
                .user_manager
                .create(&user, &credential)
                .map_err(|typedb_source| HttpServiceError::UserCreate { typedb_source })
        })
    }

    async fn users_update(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
        JsonBody(payload): JsonBody<UpdateUserPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::UsersUpdate,
            || async {
                let user_update = None; // updating username is not supported now
                let credential_update = Some(Credential::new_password(&payload.password));
                let username = user_path.username.as_str();
                if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                service
                    .user_manager
                    .update(username, &user_update, &credential_update)
                    .map_err(|typedb_source| HttpServiceError::UserUpdate { typedb_source })?;
                service.token_manager.invalidate_user(username).await;
                Ok(())
            },
        )
        .await
    }

    async fn users_delete(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::UsersDelete,
            || async {
                let username = user_path.username.as_str();
                if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                service
                    .user_manager
                    .delete(&user_path.username)
                    .map_err(|typedb_source| HttpServiceError::UserDelete { typedb_source })?;
                service.token_manager.invalidate_user(username).await;
                Ok(())
            },
        )
        .await
    }

    async fn transaction_open(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        JsonBody(payload): JsonBody<TransactionOpenPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(payload.database_name.clone()),
            ActionKind::TransactionOpen,
            || async {
                let (transaction_info, _processing_time) = Self::transaction_new(&service, accessor, payload).await?;
                let uuid = Uuid::new_v4();
                service.transaction_services.write().await.insert(uuid, transaction_info);
                Ok(JsonBody(encode_transaction(uuid)))
            },
        )
        .await
    }

    async fn transactions_commit(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let uuid = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HttpServiceError::no_open_transaction())?;

        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(transaction.database_name.clone()),
            ActionKind::TransactionCommit,
            || async {
                if accessor != transaction.owner {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                Self::transaction_request(&transaction, TransactionRequest::Commit, true).await
            },
        )
        .await
    }

    async fn transactions_close(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let uuid = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let Some(transaction) = senders.get(&uuid) else {
            return Ok(TransactionServiceResponse::Ok);
        };

        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(transaction.database_name.clone()),
            ActionKind::TransactionClose,
            || async {
                if accessor != transaction.owner {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                Self::transaction_request(&transaction, TransactionRequest::Close, false).await
            },
        )
        .await
    }

    async fn transactions_rollback(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let uuid = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HttpServiceError::no_open_transaction())?;

        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(transaction.database_name.clone()),
            ActionKind::TransactionRollback,
            || async {
                if accessor != transaction.owner {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                Self::transaction_request(&transaction, TransactionRequest::Rollback, true).await
            },
        )
        .await
    }

    async fn transactions_query(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
        JsonBody(payload): JsonBody<TransactionQueryPayload>,
    ) -> impl IntoResponse {
        let uuid = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HttpServiceError::no_open_transaction())?;

        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(transaction.database_name.clone()),
            ActionKind::TransactionQuery,
            || async {
                if accessor != transaction.owner {
                    return Err(HttpServiceError::operation_not_permitted());
                }
                Self::transaction_request(
                    &transaction,
                    Self::build_query_request(payload.query_options, payload.query),
                    true,
                )
                .await
            },
        )
        .await
    }

    async fn query(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        JsonBody(payload): JsonBody<QueryPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(payload.transaction_open_payload.database_name.clone()),
            ActionKind::OneshotQuery,
            || async {
                let (transaction_info, _processing_time) =
                    Self::transaction_new(&service, accessor, payload.transaction_open_payload).await?;

                let transaction_response = Self::transaction_request(
                    &transaction_info,
                    Self::build_query_request(payload.query_options, payload.query),
                    true,
                )
                .await?;
                let query_response = Self::try_get_query_response(transaction_response)?;

                let commit = match query_response.query_type() {
                    QueryType::Read => false,
                    QueryType::Write | QueryType::Schema => {
                        payload.commit.unwrap_or(Self::QUERY_ENDPOINT_COMMIT_DEFAULT)
                    }
                };

                let close_response = match commit {
                    true => Self::transaction_request(&transaction_info, TransactionRequest::Commit, true),
                    false => Self::transaction_request(&transaction_info, TransactionRequest::Close, true),
                }
                .await?;
                if let TransactionServiceResponse::Err(typedb_source) = close_response {
                    return match commit {
                        true => Err(HttpServiceError::QueryCommit { typedb_source }),
                        false => Err(HttpServiceError::QueryClose { typedb_source }),
                    };
                }

                Ok(TransactionServiceResponse::Query(query_response))
            },
        )
        .await
    }
}

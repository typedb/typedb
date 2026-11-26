/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use async_trait::async_trait;
use concurrency::TokioTaskSpawner;
use database::{database_manager::DatabaseManager, transaction::TransactionId};
use futures::future::join_all;
use options::TransactionOptions;
use system::concepts::DatabasePermission;
use tokio::sync::{RwLock, mpsc::Sender};

use crate::{
    authentication::Accessor,
    error::{ArcServerStateError, LocalServerStateError, arc_server_state_err},
    service::TransactionType,
    state::permission_operator::PermissionOperator,
    transaction::{Transaction, open_transaction_blocking},
};

#[derive(Debug)]
pub(crate) struct TransactionInfo {
    transaction_type: TransactionType,
    database_name: String,
    owner: String,
    close_sender: Sender<()>,
}

#[async_trait]
pub trait TransactionOperator: Debug + Send + Sync {
    async fn open(
        &self,
        accessor: &Accessor,
        database_name: &str,
        transaction_type: TransactionType,
        options: TransactionOptions,
        close_sender: Sender<()>,
    ) -> Result<Transaction, ArcServerStateError>;

    async fn has_by_database(&self, database_name: &str) -> bool;

    async fn close_by_types(&self, types: &HashSet<TransactionType>);

    async fn close_by_owner(&self, username: &str);

    async fn close_by_database(&self, database_name: &str);
}

#[derive(Debug)]
pub struct LocalTransactionOperator {
    database_manager: Arc<DatabaseManager>,
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    background_task_spawner: TokioTaskSpawner,
    permission_operator: Arc<dyn PermissionOperator>,
}

impl LocalTransactionOperator {
    pub fn new(
        database_manager: Arc<DatabaseManager>,
        background_task_spawner: TokioTaskSpawner,
        permission_operator: Arc<dyn PermissionOperator>,
    ) -> Self {
        Self {
            database_manager,
            transactions: Arc::new(RwLock::new(HashMap::new())),
            background_task_spawner,
            permission_operator,
        }
    }

    pub async fn record(
        &self,
        transaction_id: TransactionId,
        database_name: String,
        owner: String,
        transaction_type: TransactionType,
        close_sender: Sender<()>,
    ) {
        let close_sender_for_cleanup = close_sender.clone();
        let transactions_for_cleanup = self.transactions.clone();
        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction_id, TransactionInfo { transaction_type, database_name, owner, close_sender });
        self.background_task_spawner.spawn(async move {
            close_sender_for_cleanup.closed().await;
            transactions_for_cleanup.write().await.remove(&transaction_id);
        });
    }

    async fn close_matching(&self, matches: impl Fn(&TransactionInfo) -> bool) {
        let close_senders: Vec<Sender<()>> = self
            .transactions
            .write()
            .await
            .extract_if(|_, info| matches(info))
            .map(|(_, info)| info.close_sender)
            .collect();
        join_all(close_senders.iter().map(|sender| async move {
            let _ = sender.send(()).await;
            sender.closed().await;
        }))
        .await;
    }
}

#[async_trait]
impl TransactionOperator for LocalTransactionOperator {
    async fn open(
        &self,
        accessor: &Accessor,
        database_name: &str,
        transaction_type: TransactionType,
        options: TransactionOptions,
        close_sender: Sender<()>,
    ) -> Result<Transaction, ArcServerStateError> {
        let database = self.database_manager.database(database_name).ok_or_else(|| {
            arc_server_state_err(LocalServerStateError::DatabaseNotFound { name: database_name.to_string() })
        })?;
        let required_permission = match transaction_type {
            TransactionType::Read => DatabasePermission::TransactionRead,
            TransactionType::Write => DatabasePermission::TransactionWrite,
            TransactionType::Schema => DatabasePermission::SchemaTypes,
        };
        if !self
            .permission_operator
            .has_database_permission(accessor, &database.database_id(), &required_permission)
            .await?
        {
            return Err(arc_server_state_err(LocalServerStateError::PermissionDeniedGeneric {
                accessor: accessor.username().to_owned(),
            }));
        }
        let transaction =
            open_transaction_blocking(database, transaction_type, options).await.map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::TransactionOpenFailed { typedb_source })
            })?;
        self.record(
            transaction.id(),
            database_name.to_owned(),
            accessor.username().to_owned(),
            transaction_type,
            close_sender,
        )
        .await;
        Ok(transaction)
    }

    async fn has_by_database(&self, database_name: &str) -> bool {
        let transactions = self.transactions.read().await;
        transactions.values().any(|info| info.database_name == database_name)
    }

    async fn close_by_types(&self, types: &HashSet<TransactionType>) {
        self.close_matching(|info| types.contains(&info.transaction_type)).await
    }

    async fn close_by_owner(&self, username: &str) {
        self.close_matching(|info| info.owner == username).await
    }

    async fn close_by_database(&self, database_name: &str) {
        self.close_matching(|info| info.database_name == database_name).await
    }
}

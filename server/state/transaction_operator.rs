/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use concurrency::{IntervalTaskParameters, TokioTaskSpawner};
use database::{database_manager::DatabaseManager, transaction::TransactionId};
use options::TransactionOptions;
use resource::constants::common::SECONDS_IN_MINUTE;
use tokio::sync::{RwLock, mpsc::Sender};

use crate::{
    error::{ArcServerStateError, LocalServerStateError, arc_server_state_err},
    service::TransactionType,
    transaction::{Transaction, open_transaction_blocking},
};

#[derive(Debug)]
pub(crate) struct TransactionInfo {
    transaction_type: TransactionType,
    owner: String,
    close_sender: Sender<()>,
}

#[async_trait]
pub trait TransactionOperator: Debug + Send + Sync {
    async fn open(
        &self,
        database_name: &str,
        transaction_type: TransactionType,
        options: TransactionOptions,
        owner: String,
        close_sender: Sender<()>,
    ) -> Result<Transaction, ArcServerStateError>;

    async fn close_by_types(&self, types: &HashSet<TransactionType>);

    async fn close_by_owner(&self, username: &str);
}

#[derive(Debug)]
pub struct LocalTransactionOperator {
    database_manager: Arc<DatabaseManager>,
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
}

impl LocalTransactionOperator {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5 * SECONDS_IN_MINUTE);

    pub fn new(database_manager: Arc<DatabaseManager>, background_task_spawner: TokioTaskSpawner) -> Self {
        let transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>> = Arc::new(RwLock::new(HashMap::new()));
        let cleanup_transactions = transactions.clone();
        background_task_spawner.spawn_interval(
            move || {
                let transactions = cleanup_transactions.clone();
                async move {
                    let mut transactions = transactions.write().await;
                    transactions.retain(|_, info| !info.close_sender.is_closed());
                }
            },
            IntervalTaskParameters::new_with_delay(Self::CLEANUP_INTERVAL, Self::CLEANUP_INTERVAL, false),
        );
        Self { database_manager, transactions }
    }

    pub async fn record(
        &self,
        transaction_id: TransactionId,
        transaction_type: TransactionType,
        owner: String,
        close_sender: Sender<()>,
    ) {
        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction_id, TransactionInfo { transaction_type, owner, close_sender });
    }
}

#[async_trait]
impl TransactionOperator for LocalTransactionOperator {
    async fn open(
        &self,
        database_name: &str,
        transaction_type: TransactionType,
        options: TransactionOptions,
        owner: String,
        close_sender: Sender<()>,
    ) -> Result<Transaction, ArcServerStateError> {
        let database = self.database_manager.database(database_name).ok_or_else(|| {
            arc_server_state_err(LocalServerStateError::DatabaseNotFound { name: database_name.to_string() })
        })?;
        let transaction =
            open_transaction_blocking(database, transaction_type, options).await.map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::TransactionOpenFailed { typedb_source })
            })?;
        self.record(transaction.id(), transaction_type, owner, close_sender).await;
        Ok(transaction)
    }

    async fn close_by_types(&self, types: &HashSet<TransactionType>) {
        let mut transactions = self.transactions.write().await;
        let to_close: Vec<_> =
            transactions.iter().filter(|(_, info)| types.contains(&info.transaction_type)).map(|(id, _)| *id).collect();
        for id in to_close {
            if let Some(info) = transactions.remove(&id) {
                let _ = info.close_sender.send(()).await;
            }
        }
    }

    async fn close_by_owner(&self, username: &str) {
        let mut transactions = self.transactions.write().await;
        let to_close: Vec<_> =
            transactions.iter().filter(|(_, info)| username == info.owner).map(|(id, _)| *id).collect();
        for id in to_close {
            if let Some(info) = transactions.remove(&id) {
                let _ = info.close_sender.send(()).await;
            }
        }
    }
}

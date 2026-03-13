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
use database::transaction::TransactionId;
use resource::constants::common::SECONDS_IN_MINUTE;
use tokio::sync::{mpsc::Sender, RwLock};

use crate::{error::ArcServerStateError, service::TransactionType};

#[derive(Debug)]
pub(crate) struct TransactionInfo {
    transaction_type: TransactionType,
    owner: String,
    close_sender: Sender<()>,
}

#[async_trait]
pub trait ServerTransactionManager: Debug + Send + Sync {
    async fn add(
        &self,
        transaction_id: TransactionId,
        transaction_type: TransactionType,
        owner: String,
        close_sender: Sender<()>,
    ) -> Result<(), ArcServerStateError>;

    async fn close_by_types(&self, types: HashSet<TransactionType>);

    async fn close_by_owner(&self, username: &str);
}

#[derive(Debug)]
pub struct LocalServerTransactionManager {
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
}

impl LocalServerTransactionManager {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5 * SECONDS_IN_MINUTE);

    pub fn new(background_task_spawner: TokioTaskSpawner) -> Self {
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
        Self { transactions }
    }
}

#[async_trait]
impl ServerTransactionManager for LocalServerTransactionManager {
    async fn add(
        &self,
        transaction_id: TransactionId,
        transaction_type: TransactionType,
        owner: String,
        close_sender: Sender<()>,
    ) -> Result<(), ArcServerStateError> {
        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction_id, TransactionInfo { transaction_type, owner, close_sender });
        Ok(())
    }

    async fn close_by_types(&self, types: HashSet<TransactionType>) {
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
        let transactions = self.transactions.read().await;
        for (_, info) in transactions.iter() {
            if username == info.owner {
                let _ = info.close_sender.send(()).await;
            }
        }
    }
}

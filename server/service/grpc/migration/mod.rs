/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use database::transaction::TransactionRead;
use storage::durability_client::WALClient;

pub(crate) mod export_service;
pub(crate) mod import_service;
pub(crate) mod item;

// TODO: Temp, remove after merges
struct TransactionHolder(Option<TransactionRead<WALClient>>);

impl TransactionHolder {
    fn new(transaction: TransactionRead<WALClient>) -> Self {
        Self(Some(transaction))
    }

    fn transaction(&self) -> &TransactionRead<WALClient> {
        self.0.as_ref().expect("Expected transaction")
    }
}

impl Drop for TransactionHolder {
    fn drop(&mut self) {
        if let Some(transaction) = self.0.take() {
            transaction.close();
        }
    }
}

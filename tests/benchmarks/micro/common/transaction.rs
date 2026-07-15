/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::transaction::{
    CommitIntent, DataCommitError, DatabaseDropGuard, SchemaCommitError, TransactionRead, TransactionWrite,
};
use function::function_manager::FunctionManager;
use options::TransactionOptions;
use query::query_manager::QueryManager;
use resource::profile::TransactionProfile;
use storage::{
    durability_client::WALClient,
    snapshot::{ReadSnapshot, ReadableSnapshot, WriteSnapshot},
};

pub enum CommitError {
    Data(DataCommitError),
    Schema(SchemaCommitError),
}

pub(crate) struct PartialTx {
    pub(crate) type_manager: Arc<TypeManager>,
    pub(crate) thing_manager: Arc<ThingManager>,
    pub(crate) function_manager: Arc<FunctionManager>,
    pub(crate) query_manager: Arc<QueryManager>,
    pub(crate) database: DatabaseDropGuard<WALClient>,
    pub(crate) transaction_options: TransactionOptions,
    pub(crate) profile: TransactionProfile,
}

pub(crate) trait UnifiedTransactionView {
    type Snapshot: ReadableSnapshot + 'static;
    fn into_parts(self) -> (Arc<Self::Snapshot>, PartialTx);
    fn reconstruct(snapshot: Arc<Self::Snapshot>, partial_tx: PartialTx) -> Self;
    fn commit(self) -> Result<TransactionProfile, CommitError>;
}

impl UnifiedTransactionView for TransactionWrite<WALClient> {
    type Snapshot = WriteSnapshot<WALClient>;
    fn into_parts(self) -> (Arc<WriteSnapshot<WALClient>>, PartialTx) {
        let TransactionWrite {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        } = self;
        (
            snapshot,
            PartialTx {
                type_manager,
                thing_manager,
                function_manager,
                query_manager,
                database,
                transaction_options,
                profile,
            },
        )
    }

    fn reconstruct(snapshot: Arc<WriteSnapshot<WALClient>>, partial_tx: PartialTx) -> Self {
        let PartialTx {
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        } = partial_tx;
        TransactionWrite::from_parts(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        )
    }

    fn commit(self) -> Result<TransactionProfile, CommitError> {
        let (mut profile, finalise_result) = self.finalise();
        match finalise_result.and_then(|intent| intent.commit(profile.commit_profile())) {
            Ok(()) => Ok(profile),
            Err(err) => Err(CommitError::Data(err)),
        }
    }
}

impl UnifiedTransactionView for TransactionRead<WALClient> {
    type Snapshot = ReadSnapshot<WALClient>;
    fn into_parts(self) -> (Arc<Self::Snapshot>, PartialTx) {
        let TransactionRead {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        } = self;
        let partial_tx = PartialTx {
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        };
        (snapshot, partial_tx)
    }

    fn reconstruct(snapshot: Arc<Self::Snapshot>, partial_tx: PartialTx) -> Self {
        let PartialTx {
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        } = partial_tx;
        TransactionRead {
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            database,
            transaction_options,
            profile,
        }
    }

    fn commit(self) -> Result<TransactionProfile, CommitError> {
        let TransactionRead { profile, .. } = self;
        Ok(profile)
    }
}

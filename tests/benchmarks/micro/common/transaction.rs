/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::{
    Database,
    transaction::{
        CommitIntent, DataCommitError, DatabaseDropGuard, SchemaCommitError, TransactionError, TransactionRead,
        TransactionWrite,
    },
};
use function::function_manager::FunctionManager;
use options::TransactionOptions;
use query::{error::QueryError, given_rows::GivenRowsSimple, query_manager::QueryManager};
use resource::profile::TransactionProfile;
use storage::{
    durability_client::WALClient,
    snapshot::{ReadSnapshot, ReadableSnapshot, WritableSnapshot, WriteSnapshot},
};

use crate::{AnswerConsumer, QueryAnswer, execute_read_query_in, execute_write_query_in, utils::PackedResult};

#[derive(Debug)]
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

pub(crate) trait UnifiedTransactionView: Sized + 'static {
    type Snapshot: ReadableSnapshot + 'static;
    fn open(database: Arc<Database<WALClient>>, options: TransactionOptions) -> Result<Self, TransactionError>;
    fn query<AC: AnswerConsumer>(
        self,
        query: &str,
        given_rows: Option<GivenRowsSimple>,
        enable_profiling: bool,
    ) -> PackedResult<QueryAnswer<AC::Output>, Box<QueryError>, Self>;
    fn into_parts(self) -> (Arc<Self::Snapshot>, PartialTx);
    fn reconstruct(snapshot: Arc<Self::Snapshot>, partial_tx: PartialTx) -> Self;
    fn commit(self) -> Result<TransactionProfile, CommitError>;
}

pub(crate) trait WriteTransactionView: UnifiedTransactionView<Snapshot: WritableSnapshot> {}

impl UnifiedTransactionView for TransactionWrite<WALClient> {
    type Snapshot = WriteSnapshot<WALClient>;

    fn open(database: Arc<Database<WALClient>>, options: TransactionOptions) -> Result<Self, TransactionError> {
        Self::open(database, options)
    }

    fn query<AC: AnswerConsumer>(
        self,
        query: &str,
        given_rows: Option<GivenRowsSimple>,
        enable_profiling: bool,
    ) -> PackedResult<QueryAnswer<AC::Output>, Box<QueryError>, Self> {
        execute_write_query_in::<Self, AC>(self, query, given_rows, enable_profiling)
    }

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

impl WriteTransactionView for TransactionWrite<WALClient> {}

impl UnifiedTransactionView for TransactionRead<WALClient> {
    type Snapshot = ReadSnapshot<WALClient>;

    fn open(database: Arc<Database<WALClient>>, options: TransactionOptions) -> Result<Self, TransactionError> {
        Self::open(database, options)
    }

    fn query<AC: AnswerConsumer>(
        self,
        query: &str,
        given_rows: Option<GivenRowsSimple>,
        enable_profiling: bool,
    ) -> PackedResult<QueryAnswer<AC::Output>, Box<QueryError>, Self> {
        execute_read_query_in::<Self, AC>(self, query, given_rows, enable_profiling)
    }

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
        Ok(self.profile)
    }
}

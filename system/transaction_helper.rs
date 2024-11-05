use std::sync::Arc;
use database::Database;
use database::transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
use storage::durability_client::WALClient;
use options::TransactionOptions;

pub fn schema_transaction<T>(database: Arc<Database<WALClient>>, fn_: impl Fn (&TransactionSchema<WALClient>) -> T) -> Result<T, SchemaCommitError> {
    let tx: TransactionSchema<WALClient> = TransactionSchema::open(database, TransactionOptions::default());
    let result = fn_(&tx);
    tx.commit().map(|_| result)
}

pub fn read_transaction<T>(database: Arc<Database<WALClient>>, fn_: impl Fn (TransactionRead<WALClient>) -> T) -> T {
    let tx: TransactionRead<WALClient> = TransactionRead::open(database, TransactionOptions::default());
    fn_(tx)
}

pub fn write_transaction<T>(database: Arc<Database<WALClient>>, fn_: impl Fn (&TransactionWrite<WALClient>) -> T) -> Result<T, DataCommitError> {
    let tx: TransactionWrite<WALClient> = TransactionWrite::open(database, TransactionOptions::default());
    let result = fn_(&tx);
    tx.commit().map(|_| result)
}

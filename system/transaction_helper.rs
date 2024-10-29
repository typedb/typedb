use database::transaction::{DataCommitError, TransactionRead, TransactionWrite};
use storage::durability_client::WALClient;

pub fn read_transaction<T>(fn_: impl Fn (TransactionRead<WALClient>) -> T) -> T {
    todo!()
}

pub fn write_transaction<T>(fn_: impl Fn (TransactionWrite<WALClient>) -> T) -> Result<T, DataCommitError> {
    todo!()
}

use std::fmt::Formatter;
use durability::wal::WAL;

pub enum ActiveTransaction {
    Read(database::transaction::TransactionRead<WAL>),
    Write(database::transaction::TransactionWrite<WAL>),
    Schema(database::transaction::TransactionSchema<WAL>),
}

impl std::fmt::Debug for ActiveTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            ActiveTransaction::Read(_) => "Read",
            ActiveTransaction::Write(_) => "Write",
            ActiveTransaction::Schema(_) => "Schema"
        })
    }
}

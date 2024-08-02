use concept::type_::type_manager::TypeManager;
use storage::snapshot::WritableSnapshot;
use typeql::query::{schema::Define, stage::Insert};

use crate::define::DefineError;

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    insert: Insert,
) -> Result<(), DefineError> {
    todo!("Understand insert semantics before trying to write anything.")
}

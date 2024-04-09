/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::rc::Rc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use storage::snapshot::{ReadSnapshot, WriteSnapshot};

pub struct TransactionRead<'txn, 'storage: 'txn, D> {
    pub(crate) snapshot: Rc<ReadSnapshot<'storage, D>>,
    pub(crate) type_manager: Rc<TypeManager<'txn, ReadSnapshot<'storage, D>>>,
    pub(crate) thing_manager: ThingManager<'txn, ReadSnapshot<'storage, D>>,
}

impl<'txn, 'storage: 'txn, D> TransactionRead<'txn, 'storage, D> {
    pub fn type_manager(&self) -> &TypeManager<'txn, ReadSnapshot<'storage, D>> {
        &self.type_manager
    }
}

pub struct TransactionWrite<'txn, 'storage: 'txn, D> {
    pub(crate) snapshot: Rc<WriteSnapshot<'storage, D>>,
    pub(crate) type_manager: Rc<TypeManager<'txn, WriteSnapshot<'storage, D>>>,
    pub(crate) thing_manager: ThingManager<'txn, WriteSnapshot<'storage, D>>,
}

impl<'txn, 'storage: 'txn, D> TransactionWrite<'txn, 'storage, D> {
    pub fn type_manager(&self) -> &TypeManager<'txn, WriteSnapshot<'storage, D>> {
        &self.type_manager
    }

    fn commit(self) {
        // 1. validate cardinality constraints on modified relations. For those that have cardinality requirements, we must also put a lock into the snapshot.
        // 2. check attributes in modified 'has' ownerships to see if they need to be cleaned up (independent & last ownership)
    }
}

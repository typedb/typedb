/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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

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

use concept::type_manager::TypeManager;
use storage::snapshot::Snapshot;

pub(crate) struct TransactionRead<'txn, 'storage: 'txn> {
    pub(crate) snapshot: Rc<Snapshot<'storage>>,
    pub(crate) type_manager: TypeManager<'txn, 'storage>,
}

pub(crate) struct TransactionWrite<'txn, 'storage: 'txn> {
    pub(crate) snapshot: Rc<Snapshot<'storage>>,
    pub type_manager: TypeManager<'txn, 'storage>,
}

impl<'txn, 'storage: 'txn> TransactionWrite<'txn, 'storage> {



}


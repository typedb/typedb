/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::rc::Rc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::thing::edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeRelationIndex, ThingEdgeRolePlayer};
use encoding::graph::thing::vertex_attribute::AttributeVertex;
use encoding::graph::thing::vertex_object::ObjectVertex;
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadSnapshot, WriteSnapshot, SchemaSnapshot};

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

    }
}

// pub struct TransactionSchema<'txn, 'storage: 'txn, D> {
//     pub(crate) snapshot: Rc<SchemaSnapshot<'storage, D>>,
//     pub(crate) type_manager: Rc<TypeManager<'txn, SchemaSnapshot<'storage, D>>>,
//     pub(crate) thing_manager: ThingManager<'txn, SchemaSnapshot<'storage, D>>,
// }
//
// impl<'txn, 'storage: 'txn, D> TransactionSchema<'txn, 'storage, D> {
//     pub fn type_manager(&self) -> &TypeManager<'txn, SchemaSnapshot<'storage, D>> {
//         &self.type_manager
//     }
//
//     fn commit(self) {
//         // 1. validate cardinality constraints on modified relations. For those that have cardinality requirements, we must also put a lock into the snapshot.
//         // 2. check attributes in modified 'has' ownerships to see if they need to be cleaned up (independent & last ownership)
//
//         let writes = self.snapshot.iterate_writes();
//         // we can either write Things or Edges.
//
//         // TODO: move into ThingManager::commit()
//         for (key, write) in writes {
//             if ObjectVertex::is_object_vertex(StorageKeyReference::from(&key)) {
//
//             } else if AttributeVertex::is_attribute_vertex(StorageKeyReference::from(&key)) {
//
//             } else if ThingEdgeHas::is_has(StorageKeyReference::from(&key)) {
//
//             } else if ThingEdgeHasReverse::is_has_reverse(StorageKeyReference::from(&key)) {
//
//             } else if ThingEdgeRolePlayer::is_role_player(StorageKeyReference::from(&key)) {
//
//             } else if ThingEdgeRelationIndex::is_index(StorageKeyReference::from(&key)) {
//
//             } else {
//                 unreachable!("Unrecognised modified key in a data transaction.")
//             }
//         }
//     }
// }

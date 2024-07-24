/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use answer::Type;
use concept::thing::object::Object;
use crate::executor::instruction::VariableModes;
use crate::executor::VariablePosition;

pub(crate) struct RolePlayerIteratorExecutor {
    role_player: ir::pattern::constraint::RolePlayer<VariablePosition>,

    iterate_mode: TernaryIterateMode,
    variable_modes: VariableModes,
    // owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    // attribute_types: Arc<HashSet<Type>>,
    // filter_fn: crate::executor::instruction::has_executor::HasExecutorFilter,
    // owner_cache: Option<Vec<Object<'static>>>,
}

#[derive(Debug, Copy, Clone)]
enum TernaryIterateMode {
    Unbound,
    UnboundInverted,
    BoundFrom,
    BoundFromBoundTo,
}

// impl TernaryIterateMode {
//     fn new(
//         has: &ir::pattern::constraint::RolePlayer<VariablePosition>,
//         var_modes: &VariableModes,
//         sort_by: Option<VariablePosition>,
//     ) -> TernaryIterateMode {
//         debug_assert!(!var_modes.fully_bound());
//         if var_modes.fully_unbound() {
//             match sort_by {
//                 None => {
//                     // arbitrarily pick from sorted
//                     TernaryIterateMode::Unbound
//                 }
//                 Some(variable) => {
//                     if has.owner() == variable {
//                         TernaryIterateMode::UnboundSortedFrom
//                     } else {
//                         TernaryIterateMode::UnboundSortedTo
//                     }
//                 }
//             }
//         } else {
//             TernaryIterateMode::BoundFromSortedTo
//         }
//     }
// }

impl RolePlayerIteratorExecutor {
    pub(crate) fn new(
        role_player: ir::pattern::constraint::RolePlayer<VariablePosition>,
        var_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> RolePlayerIteratorExecutor {
        todo!()
    }
}

impl RolePlayerIteratorExecutor {}

/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use crate::{
    program::{
        block::{FunctionalBlock, FunctionalBlockBuilder},
        function_signature::FunctionSignatureIndex,
    },
    translation::constraints::add_statement,
    PatternDefinitionError,
};
use crate::program::function_signature::HashMapFunctionIndex;

pub fn translate_insert(
    insert: &typeql::query::stage::Insert,
) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder();
    let function_index = HashMapFunctionIndex::empty();
    for statement in &insert.statements {
        add_statement(&function_index, &mut builder.conjunction_mut().constraints_mut(), statement)?;
    }
    Ok(builder)
}
//
// pub fn translate_delete(
//     delete: &typeql::query::stage::Delete,
// ) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
//     let mut builder = FunctionalBlock::builder();
//     let function_index = HashMapFunctionIndex::empty();
//     for deletable in &delete.deletables {
//         match deletable.kind {
//             DeletableKind::Has { attribute: Variable, owner: Variable } => {
//                 add_
//             },
//             DeletableKind::Links { players: Relation, relation: Variable } => {},
//             DeletableKind::Concept { variable: Variable } => {}
//         }
//         add_statement(&function_index, &mut builder.conjunction_mut().constraints_mut(), statement)?;
//     }
//     for in &delete.
//     Ok(builder)
// }

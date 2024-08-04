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

pub fn translate_insert(
    function_index: &impl FunctionSignatureIndex,
    insert: &typeql::query::stage::Insert,
) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder();
    for statement in &insert.statements {
        add_statement(function_index, &mut builder.conjunction_mut().constraints_mut(), statement)?;
    }
    Ok(builder)
}

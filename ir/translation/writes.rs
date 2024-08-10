/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use typeql::query::stage::delete::DeletableKind;

use crate::{
    pattern::{
        constraint::{Constraint, Has, RoleName, RolePlayer},
        variable_category::VariableCategory,
        ScopeId,
    },
    program::{
        block::{BlockContext, FunctionalBlock, FunctionalBlockBuilder},
        function_signature::{FunctionSignatureIndex, HashMapFunctionIndex},
    },
    translation::constraints::{add_statement, add_typeql_relation, register_typeql_var},
    PatternDefinitionError,
};

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

pub fn translate_delete(
    delete: &typeql::query::stage::Delete,
) -> Result<(FunctionalBlockBuilder, Vec<Variable>), PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder();
    // let function_index = HashMapFunctionIndex::empty();
    let mut tmp_conjunction = builder.conjunction_mut();
    let mut constraints = tmp_conjunction.constraints_mut();
    let mut deleted_concepts = Vec::new();
    for deletable in &delete.deletables {
        match &deletable.kind {
            DeletableKind::Has { attribute, owner } => {
                let translated_owner = register_typeql_var(&mut constraints, owner)?;
                let translated_attribute = register_typeql_var(&mut constraints, attribute)?;
                constraints.add_has(translated_owner, translated_attribute)?;
            }
            DeletableKind::Links { players, relation } => {
                let translated_relation = register_typeql_var(&mut constraints, relation)?;
                add_typeql_relation(&mut constraints, translated_relation, players)?;
            }
            DeletableKind::Concept { variable } => {
                let translated_variable = constraints.get_or_declare_variable(variable.name().unwrap())?;
                deleted_concepts.push(translated_variable);
            }
        }
    }
    deleted_concepts.iter().try_for_each(|variable| {
        // We need a constraint to be able to set this:
        // builder.context_mut().set_variable_category(variable.clone(), VariableCategory::Thing, <<HERE>>)?;
        Ok(())
    })?;

    Ok((builder, deleted_concepts))
}

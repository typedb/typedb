/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::{pattern::constraint::Constraint, program::block::FunctionalBlock};

use crate::match_::inference::{
    type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
    TypeInferenceError,
};

pub enum ValidCombinations<'a> {
    Has(&'a BTreeMap<answer::Type, Vec<answer::Type>>),
    Links(&'a BTreeMap<Type, HashSet<Type>>),
}

impl<'a> ValidCombinations<'a> {
    fn check(&self, left: &answer::Type, right: &answer::Type) -> bool {
        match self {
            ValidCombinations::Has(has) => has.get(left).unwrap().contains(right),
            ValidCombinations::Links(links) => links.get(left).unwrap().contains(right),
        }
    }
}

pub fn check_annotations(
    block: &FunctionalBlock,
    input_annotations_variables: &HashMap<Variable, Arc<HashSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    insert_annotations: &TypeAnnotations<Variable>,
) -> Result<(), TypeInferenceError> {
    for constraint in block.conjunction().constraints() {
        match constraint {
            Constraint::Isa(_) => {} // Nothing to do
            Constraint::Has(has) => {
                let valid_combinations = insert_annotations
                    .constraint_annotations_of(constraint.clone())
                    .unwrap()
                    .as_left_right()
                    .left_to_right();
                validate_input_combinations_insertable(
                    constraint,
                    input_annotations_variables,
                    input_annotations_constraints,
                    &ValidCombinations::Has(&valid_combinations),
                )?;
            }
            Constraint::Links(links) => {
                let links_annotations =
                    insert_annotations.constraint_annotations_of(constraint.clone()).unwrap().as_left_right_filtered();
                validate_input_combinations_insertable(
                    constraint,
                    input_annotations_variables,
                    input_annotations_constraints,
                    &ValidCombinations::Links(&links_annotations.filters_on_left()),
                )?;
                validate_input_combinations_insertable(
                    constraint,
                    input_annotations_variables,
                    input_annotations_constraints,
                    &ValidCombinations::Links(&links_annotations.filters_on_right()),
                )?;
            }
            Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::Sub(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::Comparison(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_) => {}
        }
    }
    Ok(())
}

fn validate_input_combinations_insertable(
    constraint: &Constraint<Variable>,
    input_annotations_variables: &HashMap<Variable, Arc<HashSet<answer::Type>>>,
    input_annotations_constraints: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    valid_combinations: &ValidCombinations,
) -> Result<(), TypeInferenceError> {
    // TODO: Improve. This is extremely coarse and likely to rule out many valid combinations
    // Esp when doing queries using type variables.
    let left = input_annotations_variables.get(&constraint.left_id()).unwrap();
    let right = input_annotations_variables.get(&constraint.right_id()).unwrap();

    let mut invalid_iter = left.iter().flat_map(|left_type| {
        right
            .iter()
            .filter(|right_type| !valid_combinations.check(left_type, right_type))
            .map(|right_type| (left_type.clone(), right_type.clone()))
    });
    if let Some((left_type, right_type)) = invalid_iter.find(|_| true) {
        Err(TypeInferenceError::IllegalInsertTypes {
            constraint: constraint.clone(),
            left_type: left_type.clone(),
            right_type: right_type.clone(),
        })?;
    }

    Ok(())
}

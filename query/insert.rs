// /*
//  * This Source Code Form is subject to the terms of the Mozilla Public
//  * License, v. 2.0. If a copy of the MPL was not distributed with this
//  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
//  */
//
// use std::collections::HashMap;
// use answer::{Thing, Type};
// use concept::type_::type_manager::TypeManager;
// use storage::snapshot::WritableSnapshot;
// use answer::variable::Variable;
// use answer::variable_value::VariableValue;
// use concept::error::ConceptWriteError;
// use concept::thing::thing_manager::ThingManager;
// use encoding::value::label::Label;
// use encoding::value::value::Value;
// use ir::pattern::constraint::{Constraint, Isa};
// use traversal::executor::batch::Row;
// use traversal::executor::VariablePosition;
//
// use crate::define::filter_variants;
// use crate::SymbolResolutionError;
// use crate::util::resolve_type;
//
// pub struct InsertClause {
//     input_vars_to_positions: HashMap<Variable, VariablePosition>,
//     created_vars_to_positions: HashMap<Variable, VariablePosition>,
//
//     constraints: Vec<Constraint<Variable>>,
//     constants: HashMap<Variable, Value<'static>>,
//     types_for_isa: HashMap<Variable, answer::Type>,
//     outputs: Vec<Variable>, // Index is position
// }
//
// impl InsertClause {
//     fn try_get_from_input(input: &Row, variable: Variable) -> Option<&'_ VariableValue> {
//         todo!()
//     }
// }
//
// impl InsertClause  {
//     pub(crate) fn new(constraints: Vec<Constraint<Variable>>, types_for_isa: HashMap<Variable, answer::Type>, constants: HashMap<Variable, Value<'static>>, input_vars_to_positions: HashMap<Variable, VariablePosition>,) -> Self {
//         debug_assert!(filter_variants!(Constraint::Isa : constraints).all(|isa| {
//             types_for_isa.contains_key(&isa.thing()) || input_vars_to_positions.contains_key(&isa.thing())
//         }));
//         Self { constraints, types_for_isa, constants, input_vars_to_positions}
//     }
//
//     fn build_types_for_isa(
//         snapshot: &mut impl WritableSnapshot,
//         type_manager: &TypeManager,
//         constraints: &Vec<Constraint<Variable>>,
//     ) -> Result<HashMap<Variable, answer::Type>, SymbolResolutionError> {
//         filter_variants!(Constraint::Label: constraints).map(|label_constraint| {
//             let type_ = resolve_type(snapshot, type_manager, &Label::build(label_constraint.type_()))?;
//             (label_constraint.left(), type_)
//         }).collect::<Result<HashMap<_,_>,_>>()
//     }
// }
//
// pub(crate) fn execute(
//     snapshot: &mut impl WritableSnapshot,
//     type_manager: &TypeManager,
//     thing_manager: &ThingManager,
//     insert: &InsertClause,
//     input: Row
// ) -> Result<Row, InsertError> {
//     let new_concepts = create_new_concepts_for_isa_constraints(snapshot, type_manager, thing_manager, insert, &input)?;
//     Ok(todo!("Create new row from input & created concepts according to what's needed"))
// }
//
// fn create_new_concepts_for_isa_constraints(
//     snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager, thing_manager: &ThingManager,
//     insert: &InsertClause, input: &Row
// ) -> Result<HashMap<Variable, VariableValue>, InsertError> {
//     filter_variants!(Constraint::Isa: &insert.constraints).map(|isa| {
//         create_new_concepts_for_single_isa(snapshot, type_manager, thing_manager, insert,&input, isa)
//             .map(|thing| VariableValue::Thing(thing))
//     }).collect()
// }
//
// fn create_new_concepts_for_single_isa(
//     snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager, thing_manager: &ThingManager,
//     insert: &InsertClause, input: &Row, isa: &Isa<Variable>,
// ) -> Result<Thing, InsertError> {
//     debug_assert!(
//         insert.input_vars_to_positions.contains_key(&isa.thing()) && !insert.input_vars_to_positions.contains_key(&isa.thing()),
//         "Should be caught at pipeline construction"
//     );
//
//     let type_ = insert.types_for_isa.get(&isa.type_()).unwrap().clone();
//     match type_ {
//         Type::Entity(entity_type) => {
//             thing_manager.create_entity(snapshot, entity_type).map(|entity| Thing::Entity(entity))
//         },
//         Type::Relation(relation_type) => {
//             thing_manager.create_relation(snapshot, relation_type).map(|relation| Thing::Relation(relation))
//         },
//         Type::Attribute(attribute_type) => {
//             let value = if let Some(value) = insert.constants.get(&isa.thing()) {
//                 value
//             } else {
//                 insert.try_get_from_input(input, &isa.thing()).as_value().unwrap()
//             };
//             thing_manager.create_attribute(snapshot, attribute_type, value.clone())
//                 .map(|attribute| Thing::Attribute(attribute))
//         },
//         Type::RoleType(_) => unreachable!("Roles can't have an isa in an insert clause"),
//     }.map_err(|source| InsertError::ConceptWrite { source })
// }
//
// fn create_capability() -> Result<(), InsertError>
//
// pub enum InsertError {
//     ConceptWrite { source: ConceptWriteError },
// }

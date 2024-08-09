// use std::collections::HashMap;
//
// use answer::variable::Variable;
// use ir::pattern::constraint::Constraint;
//
// use crate::{
//     inference::type_annotations::TypeAnnotations,
//     write::{
//         insert::DeleteCompilationError,
//         write_instructions::{Has, PutAttribute, PutEntity, PutRelation, RolePlayer, ThingSource, VariableSource},
//     },
// };
//
// #[derive(Debug)]
// pub enum DeleteInstruction {
//     // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
//     Entity(PutEntity),
//     Attribute(PutAttribute),
//     Relation(PutRelation),
//     Has(Has),               // TODO: Ordering
//     RolePlayer(RolePlayer), // TODO: Ordering
// }
//
// pub struct DeletePlan {
//     pub instructions: Vec<DeleteInstruction>,
//     // pub output_row: Vec<VariableSource>, // Where to copy from
//     // pub debug_info: HashMap<VariableSource, Variable>,
// }
//
// pub fn build_delete_plan(
//     input_variables: &HashMap<Variable, u32>,
//     type_annotations: &TypeAnnotations,
//     constraints: &[Constraint<Variable>],
//     deleted_concepts: &[Variable],
// ) -> Result<DeletePlan, DeleteCompilationError> {
//     // TODO: Maybe unify all WriteCompilation errors?
//     let mut instructions = Vec::new();
//     let inserted_things = HashMap::new();
//     deleted_concepts.iter().for_each(|variable| {});
//     for constraint in constraints {
//         match constraint {
//             Constraint::Has(has) => {
//                 instructions.push(DeleteInstruction::Has(Has {
//                     owner: crate::write::insert::get_thing_source(input_variables, &inserted_things, has.owner())?,
//                     attribute: crate::write::insert::get_thing_source(
//                         input_variables,
//                         &inserted_things,
//                         has.attribute(),
//                     )?,
//                 }));
//             }
//             Constraint::RolePlayer(role_player) => {
//                 instructions.push(DeleteInstruction::RolePlayer(RolePlayer {
//                     relation: crate::write::insert::get_thing_source(
//                         input_variables,
//                         &inserted_things,
//                         role_player.relation(),
//                     )?,
//                     player: crate::write::insert::get_thing_source(
//                         input_variables,
//                         &inserted_things,
//                         role_player.player(),
//                     )?,
//                     role: crate::write::insert::get_type_source(
//                         input_variables,
//                         &inserted_things,
//                         role_player.role_type(),
//                     )?,
//                 }));
//             }
//             Constraint::Isa(_)
//             | Constraint::Label(_)
//             | Constraint::RoleName(_)
//             | Constraint::ExpressionBinding(_)
//             | Constraint::Comparison(_)
//             | Constraint::Sub(_)
//             | Constraint::FunctionCallBinding(_) => {
//                 Err(DeleteCompilationError::IllegalConstraint { constraint: constraint.clone() })
//             }
//         }
//     }
//     // To produce the output stream, we remove the deleted concepts from each map in the stream.
//
//     let output_row = input_variables
//         .iter()
//         .filter_map(|(variable, position)| {
//             if deleted_concept_variables.contains(variable) {
//                 None
//             } else {
//                 Some(VariableSource::ThingSource(ThingSource::Input(position.clone())))
//             }
//         })
//         .collect::<Vec<_>>();
//
//     Ok(DeletePlan { instructions })
// }

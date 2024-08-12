// /*
//  * This Source Code Form is subject to the terms of the Mozilla Public
//  * License, v. 2.0. If a copy of the MPL was not distributed with this
//  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
//  */
//
// use std::collections::{HashMap, HashSet};
//
// use typeql::schema::definable::Type;
// use answer::variable::Variable;
// use compiler::expression::compiled_expression::ExpressionValueType;
// use encoding::value::value_type::{ValueType, ValueTypeCategory};
//
// use ir::pattern::variable_category::{VariableCategory, VariableOptionality};
//
// use crate::match_::MatchClause;
//
// pub enum NonTerminalStage {
//     Match(MatchClause),
//     // Insert(InsertClause),
//     // Delete(DeleteClause),
//     // Put(PutClause),
//     // Update(UpdateClause),
//     // OperatorSelect(SelectOperator),
//     // OperatorDistinct(DistinctOperator),
// }
//
// impl NonTerminalStage {
//     pub(crate) fn return_descriptor(&self) -> &HashMap<Variable, (VariableCategory, VariableOptionality)> {
//         match self {
//             NonTerminalStage::Match(match_) => match_.return_descriptor(),
//         }
//     }
//
//     pub(crate) fn variable_type_annotations(&self, variable: Variable) -> Option<&HashSet<Type>> {
//         todo!()
//     }
//
//     pub(crate) fn variable_value_type(&self, variable: Variable) -> Option<ValueType> {
//         todo!()
//     }
//
//     pub(crate) fn get_named_return_variable(&self, name: &str) -> Option<Variable> {
//         todo!()
//     }
// }
//
// pub(crate) trait NonTerminalStageAPI {
//     fn return_descriptor(&self) -> &HashMap<Variable, (VariableCategory, VariableOptionality)>;
//
//     fn variable_type_annotations(&self, variable: Variable) -> Option<&HashSet<answer::Type>>;
//
//     // TODO: this should return the full value type instead of just category
//     fn expression_variable_value_type(&self, variable: Variable) -> Option<ExpressionValueType>;
//
//     fn get_named_return_variable(&self, name: &str) -> Option<Variable>;
// }
//
// pub enum TerminalStage {
//     // Fetch(FetchClause),
//     // Reduce(ReduceOperation)
// }

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    pattern::conjunction::ConjunctionBuilder,
    program::{
        block::{FunctionalBlock, FunctionalBlockBuilder},
        function_signature::FunctionSignatureIndex,
    },
    translator::constraint_builder::TypeQLConstraintBuilder,
    PatternDefinitionError,
};

pub struct TypeQLBuilder<'func, FunctionIndex: FunctionSignatureIndex> {
    function_index: &'func FunctionIndex,
}

impl<'func, FunctionIndex: FunctionSignatureIndex> TypeQLBuilder<'func, FunctionIndex> {
    pub fn build_match(
        // TODO: Accept modifiers
        function_index: &'func FunctionIndex,
        match_: &typeql::query::stage::Match,
    ) -> Result<FunctionalBlock, PatternDefinitionError> {
        let mut builder = FunctionalBlock::builder();
        let mut this = Self { function_index };
        this.add_patterns(&mut builder.conjunction_mut(), &match_.patterns)?;
        Ok(builder.finish())
    }

    pub fn build_match_but_dont_finish(
        function_index: &'func FunctionIndex,
        match_: &typeql::query::stage::Match,
    ) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
        let mut builder = FunctionalBlock::builder();
        let mut this = Self { function_index };
        this.add_patterns(&mut builder.conjunction_mut(), &match_.patterns)?;
        Ok(builder)
    }

    fn add_patterns(
        &self,
        conjunction: &mut ConjunctionBuilder<'func>,
        patterns: &Vec<typeql::pattern::Pattern>,
    ) -> Result<(), PatternDefinitionError> {
        patterns.iter().try_for_each(|pattern| match pattern {
            typeql::pattern::Pattern::Conjunction(nested) => self.add_patterns(conjunction, &nested.patterns),
            typeql::pattern::Pattern::Disjunction(disjunction) => self.add_disjunction(conjunction, disjunction),
            typeql::pattern::Pattern::Negation(negation) => self.add_negation(conjunction, negation),
            typeql::pattern::Pattern::Optional(optional) => self.add_optional(conjunction, optional),
            typeql::pattern::Pattern::Statement(statement) => {
                let mut constraint_builder =
                    TypeQLConstraintBuilder::new(conjunction.constraints_mut(), self.function_index);
                constraint_builder.add_statement(statement)
            }
        })?;
        Ok(())
    }

    fn add_disjunction(
        &self,
        conjunction: &mut ConjunctionBuilder<'func>,
        disjunction: &typeql::pattern::Disjunction,
    ) -> Result<(), PatternDefinitionError> {
        let mut disjunction_builder = conjunction.add_disjunction();
        disjunction
            .branches
            .iter()
            .try_for_each(|branch| self.add_patterns(&mut disjunction_builder.add_conjunction(), branch))?;
        Ok(())
    }

    fn add_negation(
        &self,
        conjunction: &mut ConjunctionBuilder<'func>,
        negation: &typeql::pattern::Negation,
    ) -> Result<(), PatternDefinitionError> {
        let mut negation_builder = conjunction.add_negation();
        self.add_patterns(&mut negation_builder, &negation.patterns)
    }

    fn add_optional(
        &self,
        conjunction: &mut ConjunctionBuilder<'func>,
        optional: &typeql::pattern::Optional,
    ) -> Result<(), PatternDefinitionError> {
        let mut optional_builder = conjunction.add_optional();
        self.add_patterns(&mut optional_builder, &optional.patterns)
    }
}

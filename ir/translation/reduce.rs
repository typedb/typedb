/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::{
    Variable,
    common::Spanned,
    query::stage::reduce::Reducer as TypeQLReducer,
    token::{ReduceOperatorCollect as TypeQLReduceOperatorCollect, ReduceOperatorStat as TypeQLReduceOperatorStat},
};

use crate::{
    RepresentationError,
    pattern::variable_category::VariableCategory,
    pipeline::reduce::{AssignedReduction, Reduce, Reducer},
    translation::{PipelineTranslationContext, verify_variable_available},
};

pub fn translate_reduce(
    context: &mut PipelineTranslationContext,
    typeql_reduce: &typeql::query::stage::Reduce,
) -> Result<Reduce, Box<RepresentationError>> {
    let group = match &typeql_reduce.groupby {
        None => Vec::new(),
        Some(group) => group
            .iter()
            .map(|typeql_var| verify_variable_available!(context, typeql_var => GroupByVariableNotAvailable))
            .collect::<Result<Vec<_>, _>>()?,
    };

    let mut reductions = Vec::with_capacity(typeql_reduce.reduce_assignments.len());
    for reduce_assign in &typeql_reduce.reduce_assignments {
        let reducer = build_reducer(context, &reduce_assign.reducer)?;
        let (category, returned_is_optional) = resolve_category_optionality(&reducer);
        let var_name = reduce_assign.variable.name().ok_or_else(|| {
            Box::new(RepresentationError::NonAnonymousVariableExpected { source_span: reduce_assign.variable.span() })
        })?;
        let assigned_is_optional = match &reduce_assign.variable {
            Variable::Anonymous { optional, .. } | Variable::Named { optional, .. } => optional.is_some(),
        };
        let optionality_mismatch_check = match (assigned_is_optional, returned_is_optional) {
            (true, true) | (false, false) => Ok(()),
            (true, false) => Err(RepresentationError::WronglyMarkedOptionalAssignment {
                variable: var_name.to_owned(),
                source_span: reduce_assign.variable.span(),
            }),
            (false, true) => Err(RepresentationError::UnmarkedOptionalAssignment {
                variable: var_name.to_owned(),
                source_span: reduce_assign.variable.span(),
            }),
        };
        if let Err(err) = optionality_mismatch_check {
            // TODO: This has to wait till we finalize the spec
            // use error::TypeDBError;
            // tracing::warn!(
            //     "The declared optionality of a variable assigned in a reduce stage did not match the optionality of the reducer result. This will fail in the next version:\n{}",
            //     err.format_description()
            // );
        }
        let assigned_var = context.register_reduced_variable(
            var_name,
            category,
            returned_is_optional,
            reduce_assign.variable.span(),
            reducer,
        )?;
        reductions.push(AssignedReduction::new(assigned_var, reducer));
    }

    context
        .last_stage_visible_variables
        .retain(|name, var| group.contains(var) || reductions.iter().any(|reduction| &reduction.assigned == var));
    Ok(Reduce::new(reductions, group, typeql_reduce.span()))
}

pub(super) fn resolve_category_optionality(reduce: &Reducer) -> (VariableCategory, bool) {
    match reduce {
        Reducer::Count => (VariableCategory::Value, false),
        Reducer::CountVar(_) => (VariableCategory::Value, false),
        Reducer::Sum(_) => (VariableCategory::Value, false),
        Reducer::Max(_) => (VariableCategory::Value, true),
        Reducer::Mean(_) => (VariableCategory::Value, true),
        Reducer::Median(_) => (VariableCategory::Value, true),
        Reducer::Min(_) => (VariableCategory::Value, true),
        Reducer::Std(_) => (VariableCategory::Value, true),
    }
}

pub(crate) fn build_reducer(
    context: &PipelineTranslationContext,
    reduce_value: &TypeQLReducer,
) -> Result<Reducer, Box<RepresentationError>> {
    match reduce_value {
        TypeQLReducer::Count(count) => match &count.variable {
            None => Ok(Reducer::Count),
            Some(typeql_var) => {
                let var = verify_variable_available!(context, typeql_var => ReduceVariableNotAvailable)?;
                Ok(Reducer::CountVar(var))
            }
        },
        TypeQLReducer::Stat(stat) => {
            let var = verify_variable_available!(context, stat.variable => ReduceVariableNotAvailable)?;
            match &stat.reduce_operator {
                TypeQLReduceOperatorStat::Sum => Ok(Reducer::Sum(var)),
                TypeQLReduceOperatorStat::Max => Ok(Reducer::Max(var)),
                TypeQLReduceOperatorStat::Mean => Ok(Reducer::Mean(var)),
                TypeQLReduceOperatorStat::Median => Ok(Reducer::Median(var)),
                TypeQLReduceOperatorStat::Min => Ok(Reducer::Min(var)),
                TypeQLReduceOperatorStat::Std => Ok(Reducer::Std(var)),
            }
        }
        TypeQLReducer::Collect(collect) => match &collect.reduce_operator {
            TypeQLReduceOperatorCollect::List => Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                feature: error::UnimplementedFeature::Lists,
            })),
        },
    }
}

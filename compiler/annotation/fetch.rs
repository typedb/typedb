/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap, HashSet};

use answer::{variable::Variable, Type};
use concept::type_::{attribute_type::AttributeType, OwnerAPI, TypeAPI};
use encoding::graph::type_::Kind;
use ir::{
    pattern::ParameterID,
    pipeline::{
        fetch::{
            FetchListAttributeAsList, FetchListAttributeFromList, FetchListSubFetch, FetchObject, FetchSingleAttribute,
            FetchSome,
        }, ParameterRegistry,
        VariableRegistry,
    },
    translation::PipelineTranslationContext,
};
use storage::snapshot::ReadableSnapshot;
use typeql::common::Span;

use crate::annotation::{
    function::{annotate_anonymous_function, AnnotatedFunction},
    pipeline::{annotate_stages_and_fetch, AnnotatedStage, RunningVariableAnnotations},
    AnnotationError,
};
use crate::annotation::utils::{AnnotationContext, PipelineAnnotationContext};

#[derive(Debug, Clone)]
pub struct AnnotatedFetch {
    pub object: AnnotatedFetchObject,
}

#[derive(Debug, Clone)]
pub enum AnnotatedFetchSome {
    SingleVar(Variable),
    SingleAttribute(Variable, AttributeType),
    SingleFunction(AnnotatedFunction),

    Object(Box<AnnotatedFetchObject>),

    ListFunction(AnnotatedFunction),
    ListSubFetch(AnnotatedFetchListSubFetch),
    ListAttributesAsList(Variable, AttributeType),
    ListAttributesFromList(Variable, AttributeType),
}

#[derive(Debug, Clone)]
pub enum AnnotatedFetchObject {
    Entries(HashMap<ParameterID, AnnotatedFetchSome>),
    Attributes(Variable),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFetchListSubFetch {
    pub variable_registry: VariableRegistry,
    pub input_variables: HashSet<Variable>,
    pub stages: Vec<AnnotatedStage>,
    pub fetch: AnnotatedFetch,
}

pub(crate) fn annotate_fetch(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    fetch: FetchObject,
    input_annotations: &RunningVariableAnnotations,
) -> Result<AnnotatedFetch, AnnotationError> {
    let object = annotate_object(ctx, fetch, input_annotations)?;
    Ok(AnnotatedFetch { object })
}

fn annotate_object(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    object: FetchObject,
    input_annotations: &RunningVariableAnnotations,
) -> Result<AnnotatedFetchObject, AnnotationError> {
    match object {
        FetchObject::Entries(entries, source_spans) => {
            let annotated_entries =
                annotated_object_entries(ctx, entries, source_spans, input_annotations)?;
            Ok(AnnotatedFetchObject::Entries(annotated_entries))
        }
        FetchObject::Attributes(attributes, _source_span) => Ok(AnnotatedFetchObject::Attributes(attributes)),
    }
}

fn annotated_object_entries(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    entries: HashMap<ParameterID, FetchSome>,
    entries_spans: HashMap<ParameterID, Option<Span>>,
    input_annotations: &RunningVariableAnnotations,
) -> Result<HashMap<ParameterID, AnnotatedFetchSome>, AnnotationError> {
    let mut annotated_entries = HashMap::new();
    for (key, value) in entries.into_iter() {
        let source_span = entries_spans.get(&key).cloned().flatten();
        let annotated_value =
            annotate_some(ctx, value, input_annotations, source_span)
                .map_err(|err| AnnotationError::FetchEntry {
                    key: ctx.parameters.fetch_key(&key).unwrap().clone(),
                    typedb_source: Box::new(err),
                })?;
        annotated_entries.insert(key, annotated_value);
    }
    Ok(annotated_entries)
}

fn annotate_some(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    some: FetchSome,
    input_annotations: &RunningVariableAnnotations,
    source_span: Option<Span>,
) -> Result<AnnotatedFetchSome, AnnotationError> {
    match some {
        FetchSome::SingleVar(var) => Ok(AnnotatedFetchSome::SingleVar(var)),
        FetchSome::SingleAttribute(FetchSingleAttribute { variable, attribute }) => {
            let variable_name = ctx.variable_registry
                .get_variable_name(variable)
                .expect("Expected fetched variable names to be validated during translation");
            let attribute_type = ctx
                .type_manager
                .get_attribute_type(ctx.snapshot, &attribute)
                .map_err(|err| AnnotationError::ConceptRead { typedb_source: err })?
                .ok_or_else(|| AnnotationError::FetchAttributeNotFound {
                    var: variable_name.clone(),
                    source_span,
                    attribute,
                })?;
            let owner_types = input_annotations.concepts.get(&variable).unwrap();
            validate_attribute_owned_and_scalar(ctx, variable_name, owner_types, attribute_type, source_span)?;
            Ok(AnnotatedFetchSome::SingleAttribute(variable, attribute_type))
        }
        FetchSome::SingleFunction(mut function) => {
            let annotated_function =
                annotate_anonymous_function(&mut function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures, input_annotations, source_span)
                    .map_err(|err| AnnotationError::FetchBlockFunctionInferenceError { typedb_source: err })?;
            Ok(AnnotatedFetchSome::SingleFunction(annotated_function))
        }
        FetchSome::Object(object) => {
            let object = annotate_object(ctx, *object, input_annotations)?;
            Ok(AnnotatedFetchSome::Object(Box::new(object)))
        }
        FetchSome::ListFunction(mut function) => {
            let annotated_function =
                annotate_anonymous_function(&mut function, ctx.snapshot, ctx.type_manager, ctx.annotated_function_signatures, input_annotations, source_span)
                    .map_err(|err| AnnotationError::FetchBlockFunctionInferenceError { typedb_source: err })?;
            Ok(AnnotatedFetchSome::ListFunction(annotated_function))
        }
        FetchSome::ListSubFetch(sub_fetch) => {
            let annotated_sub_fetch = annotate_sub_fetch(ctx, sub_fetch, input_annotations);
            Ok(AnnotatedFetchSome::ListSubFetch(annotated_sub_fetch?))
        }
        FetchSome::ListAttributesAsList(FetchListAttributeAsList { variable, attribute }) => {
            let variable_name = ctx.variable_registry
                .get_variable_name(variable)
                .expect("Expected fetched variable names to be validated during translation");
            let attribute_type = ctx
                .type_manager
                .get_attribute_type(ctx.snapshot, &attribute)
                .map_err(|err| AnnotationError::ConceptRead { typedb_source: err })?
                .ok_or_else(|| AnnotationError::FetchAttributeNotFound {
                    var: variable_name.clone(),
                    source_span,
                    attribute,
                })?;
            for owner_type in input_annotations.concepts.get(&variable).unwrap().iter() {
                validate_attribute_owned_and_streamable(ctx, variable_name, owner_type, attribute_type, source_span)?;
            }
            Ok(AnnotatedFetchSome::ListAttributesAsList(variable, attribute_type))
        }
        FetchSome::ListAttributesFromList(FetchListAttributeFromList { .. }) => {
            Err(AnnotationError::Unimplemented {
                description: "Fetching a list attribute is not yet supported.".to_owned(),
            })
            // // TODO: validate attribute type cardinality matches the syntax
            // Ok(AnnotatedFetchSome::ListAttributesFromList(fetch))
        }
    }
}

fn validate_attribute_owned_and_scalar(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    owner: &str,
    owner_types: &BTreeSet<Type>,
    attribute_type: AttributeType,
    source_span: Option<Span>,
) -> Result<(), AnnotationError> {
    for owner_type in owner_types {
        if let kind @ (Kind::Attribute | Kind::Role) = owner_type.kind() {
            return Err(AnnotationError::FetchSingleAttributeCannotBeOwnedByKind {
                var: owner.to_owned(),
                kind: kind.to_string(),
                attribute: attribute_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
                source_span,
            });
        }
        let object_type = owner_type.as_object_type();
        if object_type
            .get_owns_attribute(ctx.snapshot, ctx.type_manager, attribute_type)
            .map_err(|err| AnnotationError::ConceptRead { typedb_source: err })?
            .is_none()
        {
            return Err(AnnotationError::FetchSingleAttributeNotOwned {
                var: owner.to_owned(),
                owner: owner_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
                attribute: attribute_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
                source_span,
            });
        }

        let is_bounded_to_one = object_type
            .is_owned_attribute_type_bounded_to_one(ctx.snapshot, ctx.type_manager, attribute_type)
            .map_err(|err| AnnotationError::ConceptRead { typedb_source: err })?;
        if !is_bounded_to_one {
            return Err(AnnotationError::AttributeFetchCardTooHigh {
                var: owner.to_owned(),
                owner: owner_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
                attribute: attribute_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
                source_span,
            });
        }
    }
    Ok(())
}

fn validate_attribute_owned_and_streamable(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    owner: &str,
    owner_type: &Type,
    attribute_type: AttributeType,
    source_span: Option<Span>,
) -> Result<(), AnnotationError> {
    if let kind @ (Kind::Attribute | Kind::Role) = owner_type.kind() {
        return Err(AnnotationError::FetchAttributesCannotBeOwnedByKind {
            var: owner.to_owned(),
            kind: kind.to_string(),
            attribute: attribute_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
            source_span,
        });
    }

    let _ = owner_type
        .as_object_type()
        .get_owns_attribute(ctx.snapshot, ctx.type_manager, attribute_type)
        .map_err(|err| AnnotationError::ConceptRead { typedb_source: err })?
        .ok_or_else(|| AnnotationError::FetchAttributesNotOwned {
            var: owner.to_owned(),
            owner: owner_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
            attribute: attribute_type.get_label(ctx.snapshot, ctx.type_manager).unwrap().name().as_str().to_owned(),
            source_span,
        })?;
    Ok(())
}

fn annotate_sub_fetch(
    ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    sub_fetch: FetchListSubFetch,
    input_annotations: &RunningVariableAnnotations,
) -> Result<AnnotatedFetchListSubFetch, AnnotationError> {
    let FetchListSubFetch { context, input_variables, stages, fetch } = sub_fetch;
    let PipelineTranslationContext { mut variable_registry, .. } = context;
    let (annotated_stages, annotated_fetch) = annotate_stages_and_fetch(
        ctx,
        stages,
        Some(fetch),
        input_annotations.clone(),
    )?;
    Ok(AnnotatedFetchListSubFetch {
        variable_registry,
        input_variables,
        stages: annotated_stages,
        fetch: annotated_fetch.unwrap(),
    })
}

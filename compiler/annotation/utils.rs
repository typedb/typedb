/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::annotation::TypeInferenceError;
use crate::annotation::function::AnnotatedFunctionSignatures;
use answer::variable::Variable;
use concept::type_::TypeAPI;
use concept::type_::attribute_type::AttributeType;
use concept::type_::object_type::ObjectType;
use concept::type_::relation_type::RelationType;
use concept::type_::role_type::RoleType;
use concept::type_::type_manager::TypeManager;
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use storage::snapshot::ReadableSnapshot;

pub(crate) struct PipelineAnnotationContext<'a, Snapshot: ReadableSnapshot> {
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) type_manager: &'a TypeManager,
    pub(crate) annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
    pub(crate) variable_registry: &'a mut VariableRegistry,
    pub(crate) parameters: &'a ParameterRegistry,
}

impl<'a, Snapshot: ReadableSnapshot> PipelineAnnotationContext<'a, Snapshot> {
    pub(crate) fn new(
        snapshot: &'a Snapshot,
        type_manager: &'a TypeManager,
        annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
        variable_registry: &'a mut VariableRegistry,
        parameters: &'a ParameterRegistry,
    ) -> Self {
        Self { snapshot, type_manager, annotated_function_signatures, variable_registry, parameters }
    }

    pub(crate) fn to_plain_mut(
        &mut self,
    ) -> (AnnotationContext<'a, Snapshot>, &mut VariableRegistry, &ParameterRegistry) {
        let Self { snapshot, type_manager, annotated_function_signatures, variable_registry, parameters } = self;
        (
            AnnotationContext::new(snapshot, type_manager, *annotated_function_signatures),
            variable_registry,
            parameters,
        )
    }
}

pub(crate) struct AnnotationContext<'a, Snapshot: ReadableSnapshot> {
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) type_manager: &'a TypeManager,
    pub(crate) annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
}

impl<'a, Snapshot: ReadableSnapshot> AnnotationContext<'a, Snapshot> {
    pub(crate) fn new(
        snapshot: &'a Snapshot,
        type_manager: &'a TypeManager,
        annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
    ) -> Self {
        Self { snapshot, type_manager, annotated_function_signatures }
    }

    pub(crate) fn for_pipeline(
        &self,
        variable_registry: &'a mut VariableRegistry,
        parameters: &'a ParameterRegistry,
    ) -> PipelineAnnotationContext<'a, Snapshot> {
        PipelineAnnotationContext::new(
            self.snapshot,
            self.type_manager,
            self.annotated_function_signatures,
            variable_registry,
            parameters,
        )
    }
}

pub(crate) trait NameForError {
    fn name_for_error(
        &self,
        ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    ) -> Result<String, TypeInferenceError>;
}

impl NameForError for Variable {
    fn name_for_error(
        &self,
        ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    ) -> Result<String, TypeInferenceError> {
        Ok(ctx.variable_registry.get_variable_name_or_unnamed(*self).to_owned())
    }
}

macro_rules! impl_name_error_for_label {
    ( $($type_:ty,)* ) => { $(
        impl NameForError for $type_ {
            fn name_for_error(&self, ctx: &PipelineAnnotationContext<'_, impl ReadableSnapshot>) -> Result<String, TypeInferenceError> {
                match self.get_label(ctx.snapshot, ctx.type_manager) {
                    Ok(label) => Ok(label.scoped_name().as_str().to_string()),
                    Err(typedb_source) => Err(TypeInferenceError::ConceptRead { typedb_source }),
                }
            }
        }
    )*};
}

impl_name_error_for_label!(ObjectType, AttributeType, RoleType, RelationType, answer::Type,);

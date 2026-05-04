/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use concept::type_::{TypeAPI, type_manager::TypeManager};
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{TypeInferenceError, function::AnnotatedFunctionSignatures};

pub struct PipelineAnnotationContext<'a, Snapshot: ReadableSnapshot> {
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) type_manager: &'a TypeManager,
    pub(crate) annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
    pub(crate) variable_registry: &'a mut VariableRegistry,
    pub(crate) parameters: &'a ParameterRegistry,
}

impl<'a, Snapshot: ReadableSnapshot> PipelineAnnotationContext<'a, Snapshot> {
    pub fn new(
        snapshot: &'a Snapshot,
        type_manager: &'a TypeManager,
        annotated_function_signatures: &'a dyn AnnotatedFunctionSignatures,
        variable_registry: &'a mut VariableRegistry,
        parameters: &'a ParameterRegistry,
    ) -> Self {
        Self { snapshot, type_manager, annotated_function_signatures, variable_registry, parameters }
    }

    pub(crate) fn to_parts_mut(
        &mut self,
    ) -> (AnnotationContext<'a, Snapshot>, &mut VariableRegistry, &ParameterRegistry) {
        let Self { snapshot, type_manager, annotated_function_signatures, variable_registry, parameters } = self;
        (AnnotationContext::new(snapshot, type_manager, *annotated_function_signatures), variable_registry, parameters)
    }

    pub(crate) fn name_for_error(&self, variable: Variable) -> String {
        self.variable_registry.get_variable_name_or_unnamed(variable).to_owned()
    }

    pub(crate) fn label_for_error(&self, type_: impl TypeAPI) -> Result<String, TypeInferenceError> {
        match type_.get_label(self.snapshot, self.type_manager) {
            Ok(label) => Ok(label.scoped_name().as_str().to_string()),
            Err(typedb_source) => Err(TypeInferenceError::ConceptRead { typedb_source }),
        }
    }

    pub(crate) fn type_label_for_error(&self, type_: answer::Type) -> Result<String, TypeInferenceError> {
        match type_.get_label(self.snapshot, self.type_manager) {
            Ok(label) => Ok(label.scoped_name().as_str().to_string()),
            Err(typedb_source) => Err(TypeInferenceError::ConceptRead { typedb_source }),
        }
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

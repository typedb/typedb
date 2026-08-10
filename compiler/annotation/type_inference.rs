/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use answer::Type;
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::value_type::ValueType;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::TypeInferenceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeInferenceMode {
    ConcreteSubtypesOnly,    // Queries
    IncludeAbstractSubtypes, // Schema functions
    ExactAndExplicit,        // Write stages
}

pub fn resolve_value_types(
    types: &BTreeSet<Type>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<HashSet<ValueType>, TypeInferenceError> {
    types
        .iter()
        .map(|type_| match type_ {
            Type::Attribute(attribute_type) => {
                match attribute_type.get_value_type_without_source(snapshot, type_manager) {
                    Ok(None) => {
                        let label = match type_.get_label(snapshot, type_manager) {
                            Ok(label) => label.scoped_name().as_str().to_owned(),
                            Err(_) => format!("could_not_resolve__{type_}"),
                        };
                        Err(TypeInferenceError::InternalAttributeTypeWithoutValueType { label })
                    }
                    Ok(Some(value_type)) => Ok(value_type),
                    Err(source) => Err(TypeInferenceError::ConceptRead { typedb_source: source }),
                }
            }
            _ => {
                let label = match type_.get_label(snapshot, type_manager) {
                    Ok(label) => label.scoped_name().as_str().to_owned(),
                    Err(_) => format!("could_not_resolve__{type_}"),
                };
                Err(TypeInferenceError::InternalValueTypeOfNonAttributeType { label })
            }
        })
        .collect::<Result<HashSet<_>, TypeInferenceError>>()
}

pub fn get_type_annotation_from_label<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    label_value: &encoding::value::label::Label,
) -> Result<Option<Type>, Box<ConceptReadError>> {
    if let Some(t) = type_manager.get_attribute_type(snapshot, label_value)?.map(Type::Attribute) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_entity_type(snapshot, label_value)?.map(Type::Entity) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_relation_type(snapshot, label_value)?.map(Type::Relation) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_role_type(snapshot, label_value)?.map(Type::RoleType) {
        Ok(Some(t))
    } else {
        Ok(None)
    }
}

pub fn get_all_concept_types(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<Arc<BTreeSet<Type>>, Box<ConceptReadError>> {
    // TODO: Use for FunctionParameterAnnotation::AnyConcept
    let object_types = type_manager.get_object_types(snapshot)?;
    let attribute_types = type_manager.get_attribute_types(snapshot)?;
    let concept_types =
        Iterator::chain(object_types.into_iter().map(Type::from), attribute_types.into_iter().map(Type::from))
            .collect();
    Ok(Arc::new(concept_types))
}

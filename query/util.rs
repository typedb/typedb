/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use concept::{
    error::ConceptReadError,
    type_::{
        annotation::Annotation, object_type::ObjectType, type_manager::TypeManager, Capability, KindAPI, Ordering,
        TypeAPI,
    },
};
use encoding::value::label::Label;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    type_::{BuiltinValueType, NamedType},
    TypeRef, TypeRefAny,
};
use typeql::schema::definable::Struct;

use crate::{define::DefineError, SymbolResolutionError};

macro_rules! try_unwrap {
    ($variant:path = $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    };
}
pub(crate) use try_unwrap;

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
pub(crate) use filter_variants;

pub(crate) fn type_ref_to_label_and_ordering(type_ref: &TypeRefAny) -> Result<(Label<'static>, Ordering), ()> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Named(NamedType::Label(label))) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Unordered))
        }
        TypeRefAny::List(typeql::type_::List { inner: TypeRef::Named(NamedType::Label(label)), .. }) => {
            Ok((Label::parse_from(label.ident.as_str()), Ordering::Ordered))
        }
        _ => Err(()),
    }
}

pub(crate) fn resolve_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<answer::Type, SymbolResolutionError> {
    match try_resolve_type(snapshot, type_manager, label) {
        Ok(Some(type_)) => Ok(type_),
        Ok(None) => Err(SymbolResolutionError::TypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn try_resolve_type(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<Option<answer::Type>, ConceptReadError> {
    // TODO: Introduce a method on type_manager that does this in one step
    let type_ = if let Some(object_type) = type_manager.get_object_type(snapshot, label)? {
        match object_type {
            ObjectType::Entity(entity_type) => Some(answer::Type::Entity(entity_type)),
            ObjectType::Relation(relation_type) => Some(answer::Type::Relation(relation_type)),
        }
    } else if let Some(attribute_type) = type_manager.get_attribute_type(snapshot, label)? {
        Some(answer::Type::Attribute(attribute_type))
    } else if let Some(role_type) = type_manager.get_role_type(snapshot, label)? {
        Some(answer::Type::RoleType(role_type))
    } else {
        None
    };
    Ok(type_)
}

pub(crate) fn resolve_relates(
    snapshot: &impl WritableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'_>,
) -> Result<answer::Type, SymbolResolutionError> {
    match try_resolve_type(snapshot, type_manager, label) {
        Ok(Some(type_)) => Ok(type_),
        Ok(None) => Err(SymbolResolutionError::TypeNotFound { label: label.clone().into_owned() }),
        Err(source) => Err(SymbolResolutionError::UnexpectedConceptRead { source }),
    }
}

pub(crate) fn resolve_value_type(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    field_name: &NamedType,
) -> Result<encoding::value::value_type::ValueType, SymbolResolutionError> {
    match field_name {
        NamedType::Label(label) => {
            let key = type_manager.get_struct_definition_key(snapshot, label.ident.as_str());
            match key {
                Ok(Some(key)) => Ok(encoding::value::value_type::ValueType::Struct(key)),
                Ok(None) | Err(_) => {
                    Err(SymbolResolutionError::ValueTypeNotFound { name: label.ident.as_str().to_owned() })
                }
            }
        }
        NamedType::Role(scoped_label) => Err(SymbolResolutionError::IllegalValueTypeName {
            scope: scoped_label.scope.ident.as_str().to_owned(),
            name: scoped_label.name.ident.as_str().to_owned(),
        }),
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ir::translation::tokens::translate_value_type(token))
        }
    }
}

pub(crate) fn check_can_and_need_define_supertype<'a, T: TypeAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    new_supertype: T,
) -> Result<bool, DefineError> {
    if let Some(existing_supertype) =
        type_.get_supertype(snapshot, type_manager).map_err(|source| DefineError::UnexpectedConceptRead { source })?
    {
        if existing_supertype != new_supertype {
            Err(DefineError::TypeSubAlreadyDefinedButDifferent {
                label: label.clone().into_owned(),
                supertype: new_supertype
                    .get_label_cloned(snapshot, type_manager)
                    .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                    .into_owned(),
                existing_supertype: existing_supertype
                    .get_label_cloned(snapshot, type_manager)
                    .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                    .into_owned(),
            })
        } else {
            Ok(false)
        }
    } else {
        Ok(true)
    }
}

pub(crate) fn check_can_and_need_define_override<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    new_override: CAP,
) -> Result<bool, DefineError> {
    if let Some(existing_override) = &*capability
        .get_override(snapshot, type_manager)
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?
    {
        if existing_override != &new_override {
            Err(DefineError::CapabilityOverrideAlreadyDefinedButDifferent {
                label: label.clone().into_owned(),
                overridden_interface: new_override
                    .interface()
                    .get_label_cloned(snapshot, type_manager)
                    .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                    .into_owned(),
                existing_overridden_interface: existing_override
                    .interface()
                    .get_label_cloned(snapshot, type_manager)
                    .map_err(|source| DefineError::UnexpectedConceptRead { source })?
                    .into_owned(),
            })
        } else {
            Ok(false)
        }
    } else {
        Ok(true)
    }
}

pub(crate) fn type_convert_and_validate_annotation_definition_need<'a, T: KindAPI<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    type_: T,
    annotation: Annotation,
) -> Result<Option<T::AnnotationType>, DefineError> {
    let existing_annotations = type_
        .get_annotations_declared(snapshot, type_manager)
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    let converted =
        T::AnnotationType::try_from(annotation.clone()).map_err(|source| DefineError::IllegalAnnotation { source })?;

    if let Some(existing_of_category) = existing_annotations
        .iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation.category())
    {
        if existing_of_category != &converted {
            return Err(DefineError::TypeAnnotationAlreadyDefinedButDifferent {
                label: label.clone().into_owned(),
                annotation,
                existing_annotation: existing_of_category.clone().into(),
            });
        }
    }

    Ok(if existing_annotations.contains(&converted) { None } else { Some(converted) })
}

pub(crate) fn capability_convert_and_validate_annotation_definition_need<'a, CAP: Capability<'a>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    label: &Label<'a>,
    capability: CAP,
    annotation: Annotation,
) -> Result<Option<CAP::AnnotationType>, DefineError> {
    let existing_annotations = capability
        .get_annotations_declared(snapshot, type_manager)
        .map_err(|source| DefineError::UnexpectedConceptRead { source })?;
    let converted = CAP::AnnotationType::try_from(annotation.clone())
        .map_err(|source| DefineError::IllegalAnnotation { source })?;

    if let Some(existing_of_category) = existing_annotations
        .iter()
        .find(|existing_annotation| (*existing_annotation).clone().into().category() == annotation.category())
    {
        if existing_of_category != &converted {
            return Err(DefineError::CapabilityAnnotationAlreadyDefinedButDifferent {
                label: label.clone().into_owned(),
                annotation,
                existing_annotation: existing_of_category.clone().into(),
            });
        }
    }

    Ok(if existing_annotations.contains(&converted) { None } else { Some(converted) })
}

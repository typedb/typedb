/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: not sure if these go elsewhere, or are useful in lower level packges outside //query

use concept::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationDistinct,
            AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        attribute_type::AttributeType,
        entity_type::EntityType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        KindAPI,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueTypeCategory},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    annotation::{Annotation as TypeQLAnnotation, CardinalityRange},
    common::token::{Kind as TypeQLKind, ValueType},
    schema::definable::type_::Capability,
    type_::{BuiltinValueType, NamedType},
};

use crate::SymbolResolutionError;

pub(crate) fn translate_kind(typeql_kind: TypeQLKind) -> Kind {
    match typeql_kind {
        TypeQLKind::Entity => Kind::Entity,
        TypeQLKind::Relation => Kind::Relation,
        TypeQLKind::Attribute => Kind::Attribute,
        TypeQLKind::Role => Kind::Role,
    }
}

pub(crate) fn translate_annotation(typeql_kind: &TypeQLAnnotation) -> Annotation {
    match typeql_kind {
        TypeQLAnnotation::Abstract(_) => Annotation::Abstract(AnnotationAbstract),
        TypeQLAnnotation::Cardinality(cardinality) => {
            todo!("Make typeql members public")
            // let (start, end) = match cardinality.range {
            //     CardinalityRange::Exact(start) => {
            //         (start.value.parse::<u64>().unwrap(), Some(start.value.parse::<u64>().unwrap()))
            //     },
            //     CardinalityRange::Range(start, end) => {
            //         (start.value.parse::<u64>().unwrap(), end.map(|e| e.value.parse::<u64>().unwrap()))
            //     }
            // };
            // Annotation::Cardinality(AnnotationCardinality::new(start, end))
        }
        TypeQLAnnotation::Cascade(_) => Annotation::Cascade(AnnotationCascade),
        TypeQLAnnotation::Distinct(_) => Annotation::Distinct(AnnotationDistinct),

        TypeQLAnnotation::Independent(_) => Annotation::Independent(AnnotationIndependent),
        TypeQLAnnotation::Key(_) => Annotation::Key(AnnotationKey),
        TypeQLAnnotation::Range(range) => {
            todo!("Parse literals after rebasing")
        }
        TypeQLAnnotation::Regex(regex) => {
            // Annotation::Regex(AnnotationRegex::new(regex.regex.value))
            todo!("Make typeql members public")
        }
        TypeQLAnnotation::Subkey(_) => todo!(),
        TypeQLAnnotation::Unique(_) => Annotation::Unique(AnnotationUnique),
        TypeQLAnnotation::Values(values) => {
            todo!("Parse literals after rebasing")
        }
    }
}

pub(crate) fn as_value_type_category(value_type: &ValueType) -> ValueTypeCategory {
    match value_type {
        ValueType::Boolean => ValueTypeCategory::Boolean,
        ValueType::Date => ValueTypeCategory::Date,
        ValueType::DateTime => ValueTypeCategory::DateTime,
        ValueType::DateTimeTZ => ValueTypeCategory::DateTimeTZ,
        ValueType::Decimal => ValueTypeCategory::Decimal,
        ValueType::Double => ValueTypeCategory::Double,
        ValueType::Duration => ValueTypeCategory::Duration,
        ValueType::Long => ValueTypeCategory::Long,
        ValueType::String => ValueTypeCategory::String,
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
            let category = as_value_type_category(token);
            let value_type = category.try_into_value_type().unwrap(); // unwrap is safe: builtins are never struct
            Ok(value_type)
        }
    }
}

pub(crate) trait UnwrapTypeAs<'a>: KindAPI<'a> {
    fn unwrap(type_enum: answer::Type) -> Option<Self>;
    fn resolve_for(
        snapshot: &impl WritableSnapshot,
        type_manager: &TypeManager,
        typeql_label: &typeql::type_::Label,
        capability: &Capability,
    ) -> Result<Self, SymbolResolutionError> {
        let label = Label::parse_from(typeql_label.ident.as_str());
        let type_enum = resolve_type(snapshot, type_manager, &label)?;
        let actual_kind = type_enum.kind();
        match Self::unwrap(type_enum) {
            Some(type_) => Ok(type_),
            None => Err(SymbolResolutionError::KindMismatch {
                label: label.to_owned(),
                expected: Self::ROOT_KIND,
                actual: actual_kind,
                capability: capability.to_owned(),
            }),
        }
    }
}
impl<'a> UnwrapTypeAs<'a> for EntityType<'a> {
    fn unwrap(type_enum: answer::Type) -> Option<Self> {
        match type_enum {
            answer::Type::Entity(unwrapped) => Some(unwrapped),
            _ => None,
        }
    }
}
impl<'a> UnwrapTypeAs<'a> for RelationType<'a> {
    fn unwrap(type_enum: answer::Type) -> Option<Self> {
        match type_enum {
            answer::Type::Relation(unwrapped) => Some(unwrapped),
            _ => None,
        }
    }
}
impl<'a> UnwrapTypeAs<'a> for AttributeType<'a> {
    fn unwrap(type_enum: answer::Type) -> Option<Self> {
        match type_enum {
            answer::Type::Attribute(unwrapped) => Some(unwrapped),
            _ => None,
        }
    }
}
impl<'a> UnwrapTypeAs<'a> for RoleType<'a> {
    fn unwrap(type_enum: answer::Type) -> Option<Self> {
        match type_enum {
            answer::Type::RoleType(unwrapped) => Some(unwrapped),
            _ => None,
        }
    }
}

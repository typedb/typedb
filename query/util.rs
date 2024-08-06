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
        object_type::ObjectType,
        type_manager::TypeManager,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value::Value, value_type::ValueTypeCategory},
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{annotation::{Annotation as TypeQLAnnotation, CardinalityRange}, common::token::{Kind as TypeQLKind, ValueType}, type_::{BuiltinValueType, NamedType}, TypeRef, TypeRefAny};
use concept::type_::Ordering;

use crate::{define::DefineError, SymbolResolutionError};

pub(crate) fn translate_kind(typeql_kind: TypeQLKind) -> Kind {
    match typeql_kind {
        TypeQLKind::Entity => Kind::Entity,
        TypeQLKind::Relation => Kind::Relation,
        TypeQLKind::Attribute => Kind::Attribute,
        TypeQLKind::Role => Kind::Role,
    }
}

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

fn try_parse_literal_opt(literal_opt: Option<&typeql::Literal>) -> Result<Option<Value<'static>>, DefineError> {
    Ok(if let Some(literal) = literal_opt {
        Some(
            ir::translation::literal::parse_literal(literal)
                .map_err(|source| DefineError::LiteralParseError { source })?,
        )
    } else {
        None
    })
}

pub(crate) fn translate_annotation(typeql_kind: &TypeQLAnnotation) -> Result<Annotation, DefineError> {
    Ok(match typeql_kind {
        TypeQLAnnotation::Abstract(_) => Annotation::Abstract(AnnotationAbstract),
        TypeQLAnnotation::Cardinality(cardinality) => {
            let (start, end) = match &cardinality.range {
                CardinalityRange::Exact(start) => {
                    (start.value.parse::<u64>().unwrap(), Some(start.value.parse::<u64>().unwrap()))
                }
                CardinalityRange::Range(start, end) => {
                    (start.value.parse::<u64>().unwrap(), end.as_ref().map(|e| e.value.parse::<u64>().unwrap()))
                }
            };
            Annotation::Cardinality(AnnotationCardinality::new(start, end))
        }
        TypeQLAnnotation::Cascade(_) => Annotation::Cascade(AnnotationCascade),
        TypeQLAnnotation::Distinct(_) => Annotation::Distinct(AnnotationDistinct),

        TypeQLAnnotation::Independent(_) => Annotation::Independent(AnnotationIndependent),
        TypeQLAnnotation::Key(_) => Annotation::Key(AnnotationKey),
        TypeQLAnnotation::Range(range) => Annotation::Range(AnnotationRange::new(
            try_parse_literal_opt(range.min.as_ref())?,
            try_parse_literal_opt(range.max.as_ref())?,
        )),
        TypeQLAnnotation::Regex(regex) => Annotation::Regex(AnnotationRegex::new(regex.regex.value.clone())),
        TypeQLAnnotation::Subkey(_) => {
            todo!()
        }
        TypeQLAnnotation::Unique(_) => Annotation::Unique(AnnotationUnique),
        TypeQLAnnotation::Values(values) => Annotation::Values(AnnotationValues::new(
            values
                .values
                .iter()
                .map(|value| Ok(try_parse_literal_opt(Some(value))?.unwrap()))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    })
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

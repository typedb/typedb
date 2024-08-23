/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::annotation::{
    Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationDistinct,
    AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
};
use encoding::{graph::type_::Kind, value::value_type::ValueType};
use typeql::{
    annotation::{Annotation as TypeQLAnnotation, CardinalityRange},
    common::token::{Kind as TypeQLKind, ValueType as TypeQLValueType},
};

use crate::{
    translation::literal::{extract_string_literal, translate_literal},
    LiteralParseError,
};

pub fn translate_annotation(typeql_kind: &TypeQLAnnotation) -> Result<Annotation, LiteralParseError> {
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
            range.min.as_ref().map(translate_literal).transpose()?,
            range.max.as_ref().map(translate_literal).transpose()?,
        )),
        TypeQLAnnotation::Regex(regex) => {
            Annotation::Regex(AnnotationRegex::new(extract_string_literal(&regex.regex)?))
        }
        TypeQLAnnotation::Subkey(_) => {
            todo!()
        }
        TypeQLAnnotation::Unique(_) => Annotation::Unique(AnnotationUnique),
        TypeQLAnnotation::Values(values) => Annotation::Values(AnnotationValues::new(
            values.values.iter().map(translate_literal).collect::<Result<Vec<_>, _>>()?,
        )),
    })
}

pub fn translate_kind(typeql_kind: TypeQLKind) -> Kind {
    match typeql_kind {
        TypeQLKind::Entity => Kind::Entity,
        TypeQLKind::Relation => Kind::Relation,
        TypeQLKind::Attribute => Kind::Attribute,
        TypeQLKind::Role => Kind::Role,
    }
}

pub fn translate_value_type(typeql_value_type: &TypeQLValueType) -> ValueType {
    match typeql_value_type {
        TypeQLValueType::Boolean => ValueType::Boolean,
        TypeQLValueType::Date => ValueType::Date,
        TypeQLValueType::DateTime => ValueType::DateTime,
        TypeQLValueType::DateTimeTZ => ValueType::DateTimeTZ,
        TypeQLValueType::Decimal => ValueType::Decimal,
        TypeQLValueType::Double => ValueType::Double,
        TypeQLValueType::Duration => ValueType::Duration,
        TypeQLValueType::Long => ValueType::Long,
        TypeQLValueType::String => ValueType::String,
    }
}

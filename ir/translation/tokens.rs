/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::annotation::{
    Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory, AnnotationDistinct,
    AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
};
use encoding::{graph::type_::Kind, value::value_type::ValueType};
use typeql::{annotation::CardinalityRange, token};

use crate::{
    translation::literal::{translate_literal, FromTypeQLLiteral},
    LiteralParseError,
};

pub fn translate_annotation(typeql_kind: &typeql::Annotation) -> Result<Annotation, LiteralParseError> {
    Ok(match typeql_kind {
        typeql::Annotation::Abstract(_) => Annotation::Abstract(AnnotationAbstract),
        typeql::Annotation::Cardinality(cardinality) => {
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
        typeql::Annotation::Cascade(_) => Annotation::Cascade(AnnotationCascade),
        typeql::Annotation::Distinct(_) => Annotation::Distinct(AnnotationDistinct),

        typeql::Annotation::Independent(_) => Annotation::Independent(AnnotationIndependent),
        typeql::Annotation::Key(_) => Annotation::Key(AnnotationKey),
        typeql::Annotation::Range(range) => Annotation::Range(AnnotationRange::new(
            range.min.as_ref().map(translate_literal).transpose()?,
            range.max.as_ref().map(translate_literal).transpose()?,
        )),
        typeql::Annotation::Regex(regex) => {
            Annotation::Regex(AnnotationRegex::new(String::from_typeql_literal(&regex.regex)?))
        }
        typeql::Annotation::Subkey(_) => {
            todo!()
        }
        typeql::Annotation::Unique(_) => Annotation::Unique(AnnotationUnique),
        typeql::Annotation::Values(values) => Annotation::Values(AnnotationValues::new(
            values.values.iter().map(translate_literal).collect::<Result<Vec<_>, _>>()?,
        )),
    })
}

pub fn translate_annotation_category(annotation_category: token::Annotation) -> AnnotationCategory {
    match annotation_category {
        token::Annotation::Abstract => AnnotationCategory::Abstract,
        token::Annotation::Cardinality => AnnotationCategory::Cardinality,
        token::Annotation::Cascade => AnnotationCategory::Cascade,
        token::Annotation::Distinct => AnnotationCategory::Distinct,
        token::Annotation::Independent => AnnotationCategory::Independent,
        token::Annotation::Key => AnnotationCategory::Key,
        token::Annotation::Range => AnnotationCategory::Range,
        token::Annotation::Regex => AnnotationCategory::Regex,
        token::Annotation::Subkey => todo!("Subkeys are not implemented"),
        token::Annotation::Unique => AnnotationCategory::Unique,
        token::Annotation::Values => AnnotationCategory::Values,
    }
}

pub fn translate_kind(typeql_kind: token::Kind) -> Kind {
    match typeql_kind {
        token::Kind::Entity => Kind::Entity,
        token::Kind::Relation => Kind::Relation,
        token::Kind::Attribute => Kind::Attribute,
        token::Kind::Role => Kind::Role,
    }
}

pub fn translate_value_type(typeql_value_type: &token::ValueType) -> ValueType {
    match typeql_value_type {
        token::ValueType::Boolean => ValueType::Boolean,
        token::ValueType::Date => ValueType::Date,
        token::ValueType::DateTime => ValueType::DateTime,
        token::ValueType::DateTimeTZ => ValueType::DateTimeTZ,
        token::ValueType::Decimal => ValueType::Decimal,
        token::ValueType::Double => ValueType::Double,
        token::ValueType::Duration => ValueType::Duration,
        token::ValueType::Long => ValueType::Long,
        token::ValueType::String => ValueType::String,
    }
}

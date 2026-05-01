/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::annotation::{
    Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory, AnnotationDistinct,
    AnnotationDoc, AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique,
    AnnotationValues,
};
use encoding::{graph::type_::Kind, value::value_type::ValueType};
use typeql::{
    annotation::CardinalityRange,
    common::{Spanned, error::TypeQLError},
    token,
};

use crate::{
    LiteralParseError, RepresentationError,
    translation::literal::{FromTypeQLLiteral, translate_literal},
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
            Annotation::Regex(AnnotationRegex::from_typeql_literal(regex, regex.span())?)
        }
        typeql::Annotation::Subkey(_) => {
            return Err(LiteralParseError::UnimplementedLanguageFeature {
                feature: error::UnimplementedFeature::Subkey,
            });
        }
        typeql::Annotation::Unique(_) => Annotation::Unique(AnnotationUnique),
        typeql::Annotation::Values(values) => Annotation::Values(AnnotationValues::new(
            values.values.iter().map(translate_literal).collect::<Result<Vec<_>, _>>()?,
        )),
        typeql::Annotation::Doc(doc) => {
            Annotation::Doc(AnnotationDoc::new(String::from_typeql_literal(&doc.doc, doc.span())?))
        }
        typeql::Annotation::Meta(meta) => todo!("anno meta"),
    })
}

pub fn translate_annotation_category(
    annotation_category: &typeql::schema::undefinable::AnnotationCategory,
) -> Result<AnnotationCategory, LiteralParseError> {
    match annotation_category {
        typeql::schema::undefinable::AnnotationCategory::Abstract => Ok(AnnotationCategory::Abstract),
        typeql::schema::undefinable::AnnotationCategory::Cardinality => Ok(AnnotationCategory::Cardinality),
        typeql::schema::undefinable::AnnotationCategory::Cascade => Ok(AnnotationCategory::Cascade),
        typeql::schema::undefinable::AnnotationCategory::Distinct => Ok(AnnotationCategory::Distinct),
        typeql::schema::undefinable::AnnotationCategory::Independent => Ok(AnnotationCategory::Independent),
        typeql::schema::undefinable::AnnotationCategory::Key => Ok(AnnotationCategory::Key),
        typeql::schema::undefinable::AnnotationCategory::Range => Ok(AnnotationCategory::Range),
        typeql::schema::undefinable::AnnotationCategory::Regex => Ok(AnnotationCategory::Regex),
        typeql::schema::undefinable::AnnotationCategory::Subkey => {
            Err(LiteralParseError::UnimplementedLanguageFeature { feature: error::UnimplementedFeature::Subkey })
        }
        typeql::schema::undefinable::AnnotationCategory::Unique => Ok(AnnotationCategory::Unique),
        typeql::schema::undefinable::AnnotationCategory::Values => Ok(AnnotationCategory::Values),
        typeql::schema::undefinable::AnnotationCategory::Doc => Ok(AnnotationCategory::Doc),
        typeql::schema::undefinable::AnnotationCategory::Meta(meta) => {
            // TODO: AnnotationCategory span
            Ok(AnnotationCategory::Meta(String::from_typeql_literal(meta, None)?))
        }
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
        token::ValueType::Integer => ValueType::Integer,
        token::ValueType::String => ValueType::String,
    }
}

pub(crate) fn checked_identifier(ident: &typeql::Identifier) -> Result<&str, Box<RepresentationError>> {
    ident.as_str_unreserved().map_err(|_source| {
        let TypeQLError::ReservedKeywordAsIdentifier { identifier } = _source else { unreachable!() };
        Box::new(RepresentationError::ReservedKeywordAsIdentifier { source_span: identifier.span(), identifier })
    })
}

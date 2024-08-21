/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::{
        type_::{
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
            Kind,
        },
        Typed,
    },
    layout::prefix::{Prefix, Prefix::VertexAttributeType},
    value::{label::Label, value_type::ValueType},
    Prefixed,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use super::annotation::{AnnotationCategory, AnnotationRange, AnnotationRegex, AnnotationValues};
use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{attribute::Attribute, thing_manager::ThingManager},
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationError, AnnotationIndependent, DefaultFrom},
        object_type::ObjectType,
        owns::Owns,
        type_manager::TypeManager,
        KindAPI, ThingTypeAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {}

impl Hkt for AttributeType<'static> {
    type HktSelf<'a> = AttributeType<'a>;
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for AttributeType<'a> {
    const PREFIX: Prefix = VertexAttributeType;
}

impl<'a> TypeVertexEncoding<'a> for AttributeType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexAttributeType);
        if vertex.prefix() != Prefix::VertexAttributeType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexAttributeType, actual_prefix: vertex.prefix() })
        } else {
            Ok(AttributeType { vertex })
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> primitive::prefix::Prefix for AttributeType<'a> {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().as_reference().into_starts_with(other.vertex().as_reference())
    }
}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    type SelfStatic = AttributeType<'static>;

    fn new(vertex: TypeVertex<'a>) -> AttributeType<'a> {
        Self::from_vertex(vertex).unwrap()
    }

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_type_is_abstract(snapshot, self.clone())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.delete_attribute_type(snapshot, thing_manager, self.clone().into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_label(snapshot, self.clone().into_owned())
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AttributeType<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_supertype(snapshot, self.clone().into_owned())
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_supertypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }
}

impl<'a> KindAPI<'a> for AttributeType<'a> {
    type AnnotationType = AttributeTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Attribute;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<AttributeTypeAnnotation>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_annotations<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<AttributeTypeAnnotation, AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations(snapshot, self.clone().into_owned())
    }
}

impl<'a> ThingTypeAPI<'a> for AttributeType<'a> {
    type InstanceType<'b> = Attribute<'b>;
}

impl<'a> AttributeType<'a> {
    pub fn get_value_type_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        type_manager.get_attribute_type_value_type_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_value_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        type_manager
            .get_attribute_type_value_type(snapshot, self.clone().into_owned())
            .map(|value_type_opt| value_type_opt.map(|(value_type, _)| value_type))
    }

    pub fn set_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_value_type(snapshot, thing_manager, self.clone().into_owned(), value_type)
    }

    pub fn unset_value_type(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_value_type(snapshot, thing_manager, self.clone().into_owned())
    }

    pub fn get_value_type_annotations_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<AttributeTypeAnnotation>, ConceptReadError> {
        Ok(self.get_annotations_declared(snapshot, type_manager)?.into_iter().filter(|annotation| annotation.is_value_type_annotation()).map(|annotation| annotation.clone()).collect())
    }

    pub fn get_value_type_annotations(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashMap<AttributeTypeAnnotation, AttributeType<'static>>, ConceptReadError> {
        Ok(self.get_annotations(snapshot, type_manager)?.into_iter().filter(|(annotation, _)| annotation.is_value_type_annotation()).map(|(annotation, source)| (annotation.clone(), source.clone().into_owned())).collect())
    }

    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_label(snapshot, self.clone().into_owned(), label)
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        supertype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_attribute_type_supertype(snapshot, thing_manager, self.clone().into_owned(), supertype)
    }

    pub fn unset_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_attribute_type_supertype(snapshot, thing_manager, self.clone().into_owned())
    }

    pub(crate) fn is_independent(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_attribute_type_is_independent(snapshot, self.clone())
    }

    pub fn get_constraint_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationRegex>, ConceptReadError> {
        type_manager.get_attribute_type_regex(snapshot, self.clone())
    }

    pub fn get_constraint_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationRange>, ConceptReadError> {
        type_manager.get_attribute_type_range(snapshot, self.clone())
    }

    pub fn get_constraint_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationValues>, ConceptReadError> {
        type_manager.get_attribute_type_values(snapshot, self.clone())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: AttributeTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.set_annotation_abstract(snapshot, thing_manager, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.set_annotation_independent(snapshot, thing_manager, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Regex(regex) => {
                type_manager.set_annotation_regex(snapshot, thing_manager, self.clone().into_owned(), regex)?
            }
            AttributeTypeAnnotation::Range(range) => {
                type_manager.set_annotation_range(snapshot, thing_manager, self.clone().into_owned(), range)?
            }
            AttributeTypeAnnotation::Values(values) => {
                type_manager.set_annotation_values(snapshot, thing_manager, self.clone().into_owned(), values)?
            }
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        let attribute_type_annotation = AttributeTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match attribute_type_annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.unset_attribute_type_annotation_abstract(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.unset_annotation_independent(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Regex(_) => {
                type_manager.unset_annotation_regex(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Range(_) => {
                type_manager.unset_annotation_range(snapshot, self.clone().into_owned())?
            }
            AttributeTypeAnnotation::Values(_) => {
                type_manager.unset_annotation_values(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn into_owned(self) -> AttributeType<'static> {
        AttributeType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> Display for AttributeType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[AttributeType:{}]", self.vertex.type_id_())
    }
}

// --- Owned API ---
impl<'a> AttributeType<'a> {
    pub fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_for_attribute_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_for_attribute(snapshot, self.clone().into_owned())
    }

    fn get_owns_owners(&self) {
        // TODO: Why not just have owns?
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AttributeTypeAnnotation {
    Abstract(AnnotationAbstract),
    Independent(AnnotationIndependent),
    Regex(AnnotationRegex),
    Range(AnnotationRange),
    Values(AnnotationValues),
}

impl AttributeTypeAnnotation {
    // ValueTypeAnnotation is not declared and is a part of AttributeTypeAnnotation,
    // because we don't want to store annotations directly on value types.
    pub fn is_value_type_annotation(&self) -> bool {
        match self {
            | AttributeTypeAnnotation::Regex(_)
            | AttributeTypeAnnotation::Range(_)
            | AttributeTypeAnnotation::Values(_) => true,

            | AttributeTypeAnnotation::Abstract(_)
            | AttributeTypeAnnotation::Independent(_) => false,
        }
    }
}

impl TryFrom<Annotation> for AttributeTypeAnnotation {
    type Error = AnnotationError;

    fn try_from(annotation: Annotation) -> Result<AttributeTypeAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(AttributeTypeAnnotation::Abstract(annotation)),
            Annotation::Independent(annotation) => Ok(AttributeTypeAnnotation::Independent(annotation)),
            Annotation::Regex(annotation) => Ok(AttributeTypeAnnotation::Regex(annotation)),
            Annotation::Range(annotation) => Ok(AttributeTypeAnnotation::Range(annotation)),
            Annotation::Values(annotation) => Ok(AttributeTypeAnnotation::Values(annotation)),

            | Annotation::Distinct(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Cardinality(_)
            | Annotation::Cascade(_) => {
                Err(AnnotationError::UnsupportedAnnotationForAttributeType(annotation.category()))
            }
        }
    }
}

impl Into<Annotation> for AttributeTypeAnnotation {
    fn into(self) -> Annotation {
        match self {
            AttributeTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
            AttributeTypeAnnotation::Independent(annotation) => Annotation::Independent(annotation),
            AttributeTypeAnnotation::Regex(annotation) => Annotation::Regex(annotation),
            AttributeTypeAnnotation::Range(annotation) => Annotation::Range(annotation),
            AttributeTypeAnnotation::Values(annotation) => Annotation::Values(annotation),
        }
    }
}

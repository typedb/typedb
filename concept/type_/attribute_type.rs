/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    sync::Arc,
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

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{attribute::Attribute, thing_manager::ThingManager},
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCategory, AnnotationError, AnnotationIndependent,
            AnnotationRange, AnnotationRegex, AnnotationValues, DefaultFrom,
        },
        constraint::{CapabilityConstraint, TypeConstraint},
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

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
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

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_constraint_abstract(snapshot, type_manager)?.is_some())
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

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_label_arc(snapshot, self.clone().into_owned())
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
    ) -> Result<MaybeOwns<'m, HashSet<AttributeType<'static>>>, ConceptReadError> {
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
    const KIND: Kind = Kind::Attribute;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<AttributeTypeAnnotation>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<AttributeType<'static>>>>, ConceptReadError>
    where
        'a: 'static,
    {
        type_manager.get_attribute_type_constraints(snapshot, self.clone().into_owned())
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
    ) -> Result<Option<(ValueType, AttributeType<'static>)>, ConceptReadError> {
        type_manager.get_attribute_type_value_type(snapshot, self.clone().into_owned())
    }

    pub fn get_value_type_without_source(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        self.get_value_type(snapshot, type_manager)
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
        Ok(self
            .get_annotations_declared(snapshot, type_manager)?
            .into_iter()
            .filter(|annotation| annotation.is_value_type_annotation())
            .cloned()
            .collect())
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

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<TypeConstraint<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_type_abstract_constraint(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_independent(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<TypeConstraint<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_independent_constraints(snapshot, self.clone().into_owned())
    }

    pub fn is_independent(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        Ok(!self.get_constraints_independent(snapshot, type_manager)?.is_empty())
    }

    pub fn get_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<TypeConstraint<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_regex_constraints(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<TypeConstraint<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_range_constraints(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<TypeConstraint<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_values_constraints(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: AttributeTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => type_manager.set_attribute_type_annotation_abstract(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
            )?,
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
    pub fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_owns(snapshot, self.clone().into_owned())
    }

    pub fn get_owner_types<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Owns<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_owner_types(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_for_owner<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        owner_type: ObjectType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns<'static>>>>, ConceptReadError> {
        match owner_type {
            ObjectType::Entity(entity_type) => type_manager.get_entity_type_owned_attribute_type_constraints(
                snapshot,
                entity_type,
                self.clone().into_owned(),
            ),
            ObjectType::Relation(relation_type) => type_manager.get_relation_type_owned_attribute_type_constraints(
                snapshot,
                relation_type,
                self.clone().into_owned(),
            ),
        }
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
        let annotation: Annotation = self.clone().into();
        Self::is_value_type_annotation_category(annotation.category())
    }

    pub fn is_value_type_annotation_category(annotation_category: AnnotationCategory) -> bool {
        match annotation_category {
            AnnotationCategory::Regex | AnnotationCategory::Range | AnnotationCategory::Values => true,

            AnnotationCategory::Abstract
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Unique
            | AnnotationCategory::Key
            | AnnotationCategory::Cardinality
            | AnnotationCategory::Cascade => false,
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

impl From<AttributeTypeAnnotation> for Annotation {
    fn from(val: AttributeTypeAnnotation) -> Self {
        match val {
            AttributeTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
            AttributeTypeAnnotation::Independent(annotation) => Annotation::Independent(annotation),
            AttributeTypeAnnotation::Regex(annotation) => Annotation::Regex(annotation),
            AttributeTypeAnnotation::Range(annotation) => Annotation::Range(annotation),
            AttributeTypeAnnotation::Values(annotation) => Annotation::Values(annotation),
        }
    }
}

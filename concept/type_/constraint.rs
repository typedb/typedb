/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, error::Error, fmt, fmt::Debug, hash::Hash};

use encoding::value::value::Value;
use itertools::Itertools;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::type_::{
    annotation::{
        Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationDistinct, AnnotationIndependent,
        AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
    },
    Capability, KindAPI, TypeAPI
};

macro_rules! with_constraint_description {
    ($constraint_description:ident, $target_enum:ident, $default:expr, |$constraint:ident| $expr:expr) => {
        match &$constraint_description {
            ConstraintDescription::$target_enum($constraint) => $expr,
            _ => $default,
        }
    };
}

macro_rules! unwrap_constraint_description_methods {
    ($(
        fn $method_name:ident() -> $return_type:ident = $target_enum:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(self) -> Result<$return_type, ConstraintError> {
                with_constraint_description!(self, $target_enum, Err(ConstraintError::CannotUnwrapConstraint(stringify!($target_enum).to_string())), |constraint| Ok(constraint.clone()))
            }
        )*
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConstraintCategory {
    Abstract,
    Distinct,
    Independent,
    Unique,
    Cardinality,
    Regex,
    Range,
    Values,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ConstraintDescription {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Independent(AnnotationIndependent),
    Unique(AnnotationUnique),
    Cardinality(AnnotationCardinality),
    Regex(AnnotationRegex),
    Range(AnnotationRange),
    Values(AnnotationValues),
}

impl ConstraintDescription {
    pub(crate) fn from_annotation(annotation: Annotation) -> HashSet<Self> {
        static KEY_CONSTRAINTS: [ConstraintDescription; 2] = [
            ConstraintDescription::Unique(AnnotationKey::UNIQUE),
            ConstraintDescription::Cardinality(AnnotationKey::CARDINALITY),
        ];
        match annotation {
            Annotation::Abstract(annotation) => HashSet::from([ConstraintDescription::Abstract(annotation)]),
            Annotation::Distinct(annotation) => HashSet::from([ConstraintDescription::Distinct(annotation)]),
            Annotation::Independent(annotation) => HashSet::from([ConstraintDescription::Independent(annotation)]),
            Annotation::Unique(annotation) => HashSet::from([ConstraintDescription::Unique(annotation)]),
            Annotation::Cardinality(annotation) => HashSet::from([ConstraintDescription::Cardinality(annotation)]),
            Annotation::Regex(annotation) => HashSet::from([ConstraintDescription::Regex(annotation)]),
            Annotation::Range(annotation) => HashSet::from([ConstraintDescription::Range(annotation)]),
            Annotation::Values(annotation) => HashSet::from([ConstraintDescription::Values(annotation)]),

            Annotation::Key(_) => HashSet::from(KEY_CONSTRAINTS.clone()),

            // no constraints:
            Annotation::Cascade(_) => HashSet::new(),
        }
    }

    pub(crate) fn category(&self) -> ConstraintCategory {
        match self {
            ConstraintDescription::Abstract(_) => ConstraintCategory::Abstract,
            ConstraintDescription::Distinct(_) => ConstraintCategory::Distinct,
            ConstraintDescription::Independent(_) => ConstraintCategory::Independent,
            ConstraintDescription::Unique(_) => ConstraintCategory::Unique,
            ConstraintDescription::Cardinality(_) => ConstraintCategory::Cardinality,
            ConstraintDescription::Regex(_) => ConstraintCategory::Regex,
            ConstraintDescription::Range(_) => ConstraintCategory::Range,
            ConstraintDescription::Values(_) => ConstraintCategory::Values,
        }
    }

    pub(crate) fn scope(&self) -> ConstraintScope {
        match self {
            ConstraintDescription::Abstract(_) => ConstraintScope::SingleInstanceOfType,

            ConstraintDescription::Distinct(_)
            | ConstraintDescription::Independent(_)
            | ConstraintDescription::Regex(_)
            | ConstraintDescription::Range(_)
            | ConstraintDescription::Values(_) => ConstraintScope::SingleInstanceOfTypeOrSubtype,

            ConstraintDescription::Cardinality(_) => ConstraintScope::AllInstancesOfSiblingTypeOrSubtypes,

            ConstraintDescription::Unique(_) => ConstraintScope::AllInstancesOfTypeOrSubtypes,
        }
    }

    pub(crate) fn unchecked(&self) -> bool {
        match self {
            ConstraintDescription::Cardinality(cardinality) => cardinality == &AnnotationCardinality::unchecked(),
            ConstraintDescription::Independent(_) => true,
            _ => false,
        }
    }

    pub(crate) fn narrowed_by_strictly_same_type(&self, other: &ConstraintDescription) -> bool {
        self.narrowed_correctly_by(other, false)
    }

    pub(crate) fn narrowed_by_any_type(&self, other: &ConstraintDescription) -> bool {
        self.narrowed_correctly_by(other, true)
    }

    fn narrowed_correctly_by(&self, other: &ConstraintDescription, allow_different_description: bool) -> bool {
        let default = || {
            if allow_different_description {
                true
            } else {
                unreachable!("Preceding filtering by ConstraintDescription is expected before this call")
            }
        };
        match self {
            ConstraintDescription::Abstract(_) => true,
            ConstraintDescription::Distinct(_) => true,
            ConstraintDescription::Independent(_) => true,
            ConstraintDescription::Unique(_) => true,
            ConstraintDescription::Regex(regex) => {
                with_constraint_description!(other, Regex, default(), |other_regex| regex
                    .narrowed_correctly_by(other_regex))
            }
            ConstraintDescription::Cardinality(cardinality) => {
                with_constraint_description!(other, Cardinality, default(), |other_cardinality| cardinality
                    .narrowed_correctly_by(other_cardinality))
            }
            ConstraintDescription::Range(range) => {
                with_constraint_description!(other, Range, default(), |other_range| range
                    .narrowed_correctly_by(other_range))
            }
            ConstraintDescription::Values(values) => {
                with_constraint_description!(other, Values, default(), |other_values| values
                    .narrowed_correctly_by(other_values))
            }
        }
    }

    unwrap_constraint_description_methods! {
        fn unwrap_abstract() -> AnnotationAbstract = Abstract;
        fn unwrap_distinct() -> AnnotationDistinct = Distinct;
        fn unwrap_independent() -> AnnotationIndependent = Independent;
        fn unwrap_unique() -> AnnotationUnique = Unique;
        fn unwrap_regex() -> AnnotationRegex = Regex;
        fn unwrap_cardinality() -> AnnotationCardinality = Cardinality;
        fn unwrap_range() -> AnnotationRange = Range;
        fn unwrap_values() -> AnnotationValues = Values;
    }
}

pub trait Constraint<T>: Sized + Clone + Hash + Eq {
    fn description(&self) -> ConstraintDescription;

    fn source(&self) -> T;

    fn category(&self) -> ConstraintCategory {
        self.description().category()
    }

    fn scope(&self) -> ConstraintScope {
        self.description().scope()
    }

    fn unchecked(&self) -> bool {
        self.description().unchecked()
    }

    fn validate_narrowed_by_strictly_same_type(&self, other: &ConstraintDescription) -> Result<(), ConstraintError> {
        match self.description().narrowed_by_strictly_same_type(other) {
            true => Ok(()),
            false => Err(ConstraintError::IsNotNarrowedBy { first: self.description(), second: other.clone() }),
        }
    }

    fn validate_narrowed_by_any_type(&self, other: &ConstraintDescription) -> Result<(), ConstraintError> {
        match self.description().narrowed_by_any_type(other) {
            true => Ok(()),
            false => Err(ConstraintError::IsNotNarrowedBy { first: self.description(), second: other.clone() }),
        }
    }

    fn validate_narrows_strictly_same_type(&self, other: &ConstraintDescription) -> Result<(), ConstraintError> {
        match other.narrowed_by_strictly_same_type(&self.description()) {
            true => Ok(()),
            false => Err(ConstraintError::IsNotNarrowedBy { first: other.clone(), second: self.description() }),
        }
    }

    fn validate_narrows_any_type(&self, other: &ConstraintDescription) -> Result<(), ConstraintError> {
        match other.narrowed_by_any_type(&self.description()) {
            true => Ok(()),
            false => Err(ConstraintError::IsNotNarrowedBy { first: other.clone(), second: self.description() }),
        }
    }

    fn validate_cardinality(&self, count: u64) -> Result<(), ConstraintError> {
        let cardinality = self.description().unwrap_cardinality()?;
        match cardinality.value_valid(count) {
            true => Ok(()),
            false => Err(ConstraintError::ViolatedCardinality { cardinality, count }),
        }
    }

    fn validate_regex(&self, value: Value<'_>) -> Result<(), ConstraintError> {
        match &value {
            Value::String(string_value) => {
                let regex = self.description().unwrap_regex()?;
                match regex.value_valid(string_value) {
                    true => Ok(()),
                    false => Err(ConstraintError::ViolatedRegex { regex, value: value.into_owned() }),
                }
            }
            _ => Err(ConstraintError::CorruptConstraintIsNotApplicableToValue {
                description: self.description(),
                value: value.into_owned(),
            }),
        }
    }

    fn validate_range(&self, value: Value<'_>) -> Result<(), ConstraintError> {
        let range = self.description().unwrap_range()?;
        match range.value_valid(value.as_reference()) {
            true => Ok(()),
            false => Err(ConstraintError::ViolatedRange { range, value: value.into_owned() }),
        }
    }

    fn validate_values(&self, value: Value<'_>) -> Result<(), ConstraintError> {
        let values = self.description().unwrap_values()?;
        match values.value_valid(value.as_reference()) {
            true => Ok(()),
            false => Err(ConstraintError::ViolatedValues { values, value: value.into_owned() }),
        }
    }

    fn validate_distinct(count: u64) -> Result<(), ConstraintError> {
        match count > 1 {
            false => Ok(()),
            true => Err(ConstraintError::ViolatedDistinct { count: count }),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TypeConstraint<T: KindAPI<'static>> {
    description: ConstraintDescription,
    source: T,
}

impl<T: KindAPI<'static>> TypeConstraint<T> {
    pub(crate) fn new(description: ConstraintDescription, source: T) -> Self {
        Self { description, source }
    }
}

impl<T: KindAPI<'static>> Constraint<T> for TypeConstraint<T> {
    fn description(&self) -> ConstraintDescription {
        self.description.clone()
    }

    fn source(&self) -> T {
        self.source.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CapabilityConstraint<CAP: Capability<'static>> {
    description: ConstraintDescription,
    source: CAP,
}

impl<CAP: Capability<'static>> CapabilityConstraint<CAP> {
    pub(crate) fn new(description: ConstraintDescription, source: CAP) -> Self {
        Self { description, source }
    }
}

impl<CAP: Capability<'static>> Constraint<CAP> for CapabilityConstraint<CAP> {
    fn description(&self) -> ConstraintDescription {
        self.description.clone()
    }

    fn source(&self) -> CAP {
        self.source.clone()
    }
}

// Siblings = both interface types i1 and i2 are capabilities of the same capability type (owns/plays/relates)
// of the same object type (e.g. they are owned by the same type, they are played by the same type)
// with "i1 isa $x; i2 isa $x;"
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ConstraintScope {
    SingleInstanceOfType,
    SingleInstanceOfTypeOrSubtype,
    AllInstancesOfSiblingTypeOrSubtypes,
    AllInstancesOfTypeOrSubtypes,
}

macro_rules! filter_by_constraint_category {
    ($constraints_iter:expr, $constraint_category:ident) => {
        $constraints_iter.filter(|constraint| {
            constraint.category() == crate::type_::constraint::ConstraintCategory::$constraint_category
        })
    };
}
pub(crate) use filter_by_constraint_category;

macro_rules! filter_by_source {
    ($constraints_iter:expr, $source:expr) => {
        $constraints_iter.filter(|constraint| &constraint.source() == &$source)
    };
}
pub(crate) use filter_by_source;

macro_rules! filter_out_unchecked_constraints {
    ($constraints_iter:expr) => {
        $constraints_iter.filter(|constraint| !constraint.unchecked())
    };
}
pub(crate) use filter_out_unchecked_constraints;

use crate::type_::{owns::Owns, plays::Plays, relates::Relates, type_manager::TypeManager, Ordering};

pub(crate) fn get_cardinality_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Cardinality).collect()
}

pub(crate) fn get_cardinality_constraint_opt<'a, C: Constraint<T> + 'a, T: Hash + Eq>(
    source: T,
    constraints: impl IntoIterator<Item = &'a C>,
) -> Option<C> {
    filter_by_constraint_category!(constraints.into_iter(), Cardinality)
        .filter_map(|constraint| match &constraint.source() == &source {
            true => Some(constraint.clone()),
            false => None,
        })
        .next()
}

pub(crate) fn get_cardinality_constraint<'a, CAP: Capability<'static>>(
    capability: CAP,
    constraints: impl IntoIterator<Item = &'a CapabilityConstraint<CAP>>,
) -> CapabilityConstraint<CAP> {
    get_cardinality_constraint_opt(capability, constraints)
        .expect("Expected a cardinality constraint for each capability")
}

pub(crate) fn get_abstract_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Abstract).collect()
}

pub(crate) fn get_abstract_constraint<'a, C: Constraint<T> + 'a, T: Hash + Eq>(
    source: T,
    constraints: impl IntoIterator<Item = &'a C>,
) -> Option<C> {
    let mut abstracts = filter_by_constraint_category!(constraints.into_iter(), Abstract).into_iter();
    if let Some(constraint) = abstracts.next() {
        debug_assert!(
            abstracts.next().is_none(),
            "Expected to retrieve only one abstract constraint from the type itself"
        );
        debug_assert!(
            &constraint.source() == &source,
            "Unexpected different source of an abstract constraint for type"
        );
        Some(constraint.clone())
    } else {
        None
    }
}

pub(crate) fn get_unique_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Unique).collect()
}

pub(crate) fn get_unique_constraint<'a, C: Constraint<T> + 'a, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = &'a C>,
) -> Option<C> {
    let mut uniques = filter_by_constraint_category!(constraints.into_iter(), Unique).into_iter();
    if let Some(constraint) = uniques.next() {
        debug_assert!(uniques.next().is_none(), "Expected to inherit only one unique constraint from its root source");
        Some(constraint.clone())
    } else {
        None
    }
}

pub(crate) fn get_distinct_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Distinct).collect()
}

pub(crate) fn get_independent_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Independent).collect()
}

pub(crate) fn get_regex_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Regex).collect()
}

pub(crate) fn get_range_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Range).collect()
}

pub(crate) fn get_values_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Values).collect()
}

pub(crate) fn get_checked_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_out_unchecked_constraints!(constraints.into_iter()).collect()
}

pub(crate) fn get_owns_default_constraints<CAP: Capability<'static>>(
    source: CAP,
    ordering: Ordering,
) -> HashSet<CapabilityConstraint<CAP>> {
    let mut constraints = HashSet::from([CapabilityConstraint::new(
        ConstraintDescription::Cardinality(Owns::get_default_cardinality(ordering)),
        source.clone(),
    )]);

    if let Some(default_distinct) = Owns::get_default_distinct(ordering) {
        constraints.insert(CapabilityConstraint::new(ConstraintDescription::Distinct(default_distinct), source));
    }

    constraints
}

pub(crate) fn get_plays_default_constraints<CAP: Capability<'static>>(
    source: CAP,
) -> HashSet<CapabilityConstraint<CAP>> {
    HashSet::from([CapabilityConstraint::new(
        ConstraintDescription::Cardinality(Plays::get_default_cardinality()),
        source.clone(),
    )])
}

pub(crate) fn get_relates_default_constraints<CAP: Capability<'static>>(
    source: CAP,
    role_ordering: Ordering,
) -> HashSet<CapabilityConstraint<CAP>> {
    let mut constraints = HashSet::from([CapabilityConstraint::new(
        ConstraintDescription::Cardinality(Relates::get_default_cardinality(role_ordering)),
        source.clone(),
    )]);

    if let Some(default_distinct) = Relates::get_default_distinct(role_ordering) {
        constraints.insert(CapabilityConstraint::new(ConstraintDescription::Distinct(default_distinct), source));
    }

    constraints
}

pub(crate) fn type_get_constraints_closest_source<'a, T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    constraints: impl IntoIterator<Item = &'a TypeConstraint<T>>,
) -> Option<T> {
    constraints
        .into_iter()
        .map(|constraint| constraint.source())
        .sorted_by(|lhs, rhs| {
            lhs.inheritance_cmp(snapshot, type_manager, rhs.clone()).unwrap_or(std::cmp::Ordering::Equal)
        })
        .next()
}

#[derive(Debug, Clone)]
pub enum ConstraintError {
    CannotUnwrapConstraint(String),
    CorruptConstraintIsNotApplicableToValue { description: ConstraintDescription, value: Value<'static> },
    IsNotNarrowedBy { first: ConstraintDescription, second: ConstraintDescription },
    ViolatedAbstract,
    ViolatedCardinality { cardinality: AnnotationCardinality, count: u64 },
    ViolatedRegex { regex: AnnotationRegex, value: Value<'static> },
    ViolatedRange { range: AnnotationRange, value: Value<'static> },
    ViolatedValues { values: AnnotationValues, value: Value<'static> },
    ViolatedUnique { value: Value<'static> },
    ViolatedDistinct { count: u64 },
}

impl fmt::Display for ConstraintError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConstraintError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CannotUnwrapConstraint(_) => None,
            Self::CorruptConstraintIsNotApplicableToValue { .. } => None,
            Self::IsNotNarrowedBy { .. } => None,
            Self::ViolatedAbstract => None,
            Self::ViolatedCardinality { .. } => None,
            Self::ViolatedRegex { .. } => None,
            Self::ViolatedRange { .. } => None,
            Self::ViolatedValues { .. } => None,
            Self::ViolatedUnique { .. } => None,
            Self::ViolatedDistinct { .. } => None,
        }
    }
}

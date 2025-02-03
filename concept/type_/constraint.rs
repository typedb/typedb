/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt, hash::Hash};

use encoding::value::value::Value;
use error::typedb_error;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::type_::{
    annotation::{
        Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationDistinct,
        AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
    },
    owns::Owns,
    plays::Plays,
    relates::Relates,
    type_manager::TypeManager,
    Capability, KindAPI, Ordering,
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
            pub fn $method_name(self) -> Result<$return_type, Box<ConstraintError>> {
                with_constraint_description!(
                    self,
                    $target_enum,
                    Err(Box::new(ConstraintError::CannotUnwrap { type_: stringify!($target_enum) })),
                    |constraint| Ok(constraint.clone())
                )
            }
        )*
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
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

impl fmt::Display for ConstraintCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for ConstraintCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Abstract => write!(f, "{}", AnnotationCategory::Abstract),
            Self::Distinct => write!(f, "{}", AnnotationCategory::Distinct),
            Self::Independent => write!(f, "{}", AnnotationCategory::Independent),
            Self::Unique => write!(f, "{}", AnnotationCategory::Unique),
            Self::Cardinality => write!(f, "{}", AnnotationCategory::Cardinality),
            Self::Regex => write!(f, "{}", AnnotationCategory::Regex),
            Self::Range => write!(f, "{}", AnnotationCategory::Range),
            Self::Values => write!(f, "{}", AnnotationCategory::Values),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
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
    pub fn from_annotation(annotation: Annotation) -> HashSet<Self> {
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

    pub(crate) fn requires_validation(&self) -> bool {
        match self {
            ConstraintDescription::Cardinality(cardinality) => cardinality.requires_validation(),
            ConstraintDescription::Independent(_) => false,
            _ => true,
        }
    }

    pub(crate) fn requires_operation_time_validation(&self) -> bool {
        match self {
            ConstraintDescription::Cardinality(_) => false, // only commit time
            ConstraintDescription::Independent(_) => false, // never
            _ => true,
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

impl fmt::Display for ConstraintDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for ConstraintDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintDescription::Abstract(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Distinct(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Independent(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Unique(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Cardinality(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Regex(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Range(annotation) => write!(f, "{}", annotation),
            ConstraintDescription::Values(annotation) => write!(f, "{}", annotation),
        }
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

    fn requires_validation(&self) -> bool {
        self.description().requires_validation()
    }

    fn requires_operation_time_validation(&self) -> bool {
        self.description().requires_operation_time_validation()
    }

    fn validate_narrowed_by_strictly_same_type(
        &self,
        other: &ConstraintDescription,
    ) -> Result<(), Box<ConstraintError>> {
        match self.description().narrowed_by_strictly_same_type(other) {
            true => Ok(()),
            false => {
                Err(Box::new(ConstraintError::IsNotNarrowedBy { first: self.description(), second: other.clone() }))
            }
        }
    }

    fn validate_narrowed_by_any_type(&self, other: &ConstraintDescription) -> Result<(), Box<ConstraintError>> {
        match self.description().narrowed_by_any_type(other) {
            true => Ok(()),
            false => {
                Err(Box::new(ConstraintError::IsNotNarrowedBy { first: self.description(), second: other.clone() }))
            }
        }
    }

    fn validate_narrows_strictly_same_type(&self, other: &ConstraintDescription) -> Result<(), Box<ConstraintError>> {
        match other.narrowed_by_strictly_same_type(&self.description()) {
            true => Ok(()),
            false => {
                Err(Box::new(ConstraintError::IsNotNarrowedBy { first: other.clone(), second: self.description() }))
            }
        }
    }

    fn validate_narrows_any_type(&self, other: &ConstraintDescription) -> Result<(), Box<ConstraintError>> {
        match other.narrowed_by_any_type(&self.description()) {
            true => Ok(()),
            false => {
                Err(Box::new(ConstraintError::IsNotNarrowedBy { first: other.clone(), second: self.description() }))
            }
        }
    }

    fn validate_cardinality(&self, count: u64) -> Result<(), Box<ConstraintError>> {
        let cardinality = self.description().unwrap_cardinality()?;
        match cardinality.value_valid(count) {
            true => Ok(()),
            false => Err(Box::new(ConstraintError::ViolatedCardinality { cardinality, count })),
        }
    }

    fn validate_regex(&self, value: Value<'_>) -> Result<(), Box<ConstraintError>> {
        match &value {
            Value::String(string_value) => {
                let regex = self.description().unwrap_regex()?;
                match regex.value_valid(string_value) {
                    true => Ok(()),
                    false => Err(Box::new(ConstraintError::ViolatedRegex { regex, value: value.into_owned() })),
                }
            }
            _ => Err(Box::new(ConstraintError::CorruptConstraintIsNotApplicableToValue {
                description: self.description(),
                value: value.into_owned(),
            })),
        }
    }

    fn validate_range(&self, value: Value<'_>) -> Result<(), Box<ConstraintError>> {
        let range = self.description().unwrap_range()?;
        match range.value_valid(value.as_reference()) {
            true => Ok(()),
            false => Err(Box::new(ConstraintError::ViolatedRange { range, value: value.into_owned() })),
        }
    }

    fn validate_values(&self, value: Value<'_>) -> Result<(), Box<ConstraintError>> {
        let values = self.description().unwrap_values()?;
        match values.value_valid(value.as_reference()) {
            true => Ok(()),
            false => Err(Box::new(ConstraintError::ViolatedValues { values, value: value.into_owned() })),
        }
    }

    fn validate_distinct(count: u64) -> Result<(), Box<ConstraintError>> {
        match count > 1 {
            false => Ok(()),
            true => Err(Box::new(ConstraintError::ViolatedDistinct { count })),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TypeConstraint<T: KindAPI> {
    description: ConstraintDescription,
    source: T,
}

impl<T: KindAPI> TypeConstraint<T> {
    pub(crate) fn new(description: ConstraintDescription, source: T) -> Self {
        Self { description, source }
    }
}

impl<T: KindAPI> Constraint<T> for TypeConstraint<T> {
    fn description(&self) -> ConstraintDescription {
        self.description.clone()
    }

    fn source(&self) -> T {
        self.source
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CapabilityConstraint<CAP: Capability> {
    description: ConstraintDescription,
    source: CAP,
}

impl<CAP: Capability> CapabilityConstraint<CAP> {
    pub(crate) fn new(description: ConstraintDescription, source: CAP) -> Self {
        Self { description, source }
    }
}

impl<CAP: Capability> Constraint<CAP> for CapabilityConstraint<CAP> {
    fn description(&self) -> ConstraintDescription {
        self.description.clone()
    }

    fn source(&self) -> CAP {
        self.source
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

macro_rules! filter_by_scope {
    ($constraints_iter:expr, $scope:expr) => {
        $constraints_iter.filter(|constraint| constraint.scope() == $scope)
    };
}
pub(crate) use filter_by_scope;

macro_rules! filter_out_unchecked_constraints {
    ($constraints_iter:expr) => {
        $constraints_iter.filter(|constraint| constraint.requires_validation())
    };
}

macro_rules! filter_out_operation_time_unchecked_constraints {
    ($constraints_iter:expr) => {
        $constraints_iter.filter(|constraint| constraint.requires_operation_time_validation())
    };
}
pub(crate) use filter_out_operation_time_unchecked_constraints;

pub(crate) fn get_cardinality_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Cardinality).collect()
}

pub(crate) fn get_cardinality_constraint<'a, CAP: Capability>(
    capability: CAP,
    constraints: impl IntoIterator<Item = &'a CapabilityConstraint<CAP>>,
) -> Option<CapabilityConstraint<CAP>> {
    filter_by_constraint_category!(constraints.into_iter(), Cardinality)
        .filter_map(|constraint| match constraint.source() == capability {
            true => Some(constraint.clone()),
            false => None,
        })
        .next()
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
    let mut abstracts = filter_by_constraint_category!(constraints.into_iter(), Abstract);
    if let Some(constraint) = abstracts.next() {
        debug_assert!(
            abstracts.next().is_none(),
            "Expected to retrieve only one abstract constraint from the type itself"
        );
        debug_assert!(constraint.source() == source, "Unexpected different source of an abstract constraint for type");
        Some(constraint.clone())
    } else {
        None
    }
}

#[expect(unused)]
pub(crate) fn get_unique_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_by_constraint_category!(constraints.into_iter(), Unique).collect()
}

pub(crate) fn get_unique_constraint<'a, C: Constraint<T> + 'a, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = &'a C>,
) -> Option<C> {
    let mut uniques = filter_by_constraint_category!(constraints.into_iter(), Unique);
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

pub(crate) fn get_operation_time_checked_constraints<C: Constraint<T>, T: Hash + Eq>(
    constraints: impl IntoIterator<Item = C>,
) -> HashSet<C> {
    filter_out_operation_time_unchecked_constraints!(constraints.into_iter()).collect()
}

pub(crate) fn get_owns_default_constraints<CAP: Capability>(
    source: CAP,
    ordering: Ordering,
) -> HashSet<CapabilityConstraint<CAP>> {
    let mut constraints = HashSet::from([CapabilityConstraint::new(
        ConstraintDescription::Cardinality(Owns::get_default_cardinality(ordering)),
        source,
    )]);

    if let Some(default_distinct) = Owns::get_default_distinct(ordering) {
        constraints.insert(CapabilityConstraint::new(ConstraintDescription::Distinct(default_distinct), source));
    }

    constraints
}

pub(crate) fn get_plays_default_constraints<CAP: Capability>(source: CAP) -> HashSet<CapabilityConstraint<CAP>> {
    HashSet::from([CapabilityConstraint::new(
        ConstraintDescription::Cardinality(Plays::get_default_cardinality()),
        source,
    )])
}

pub(crate) fn get_relates_default_constraints<CAP: Capability>(
    source: CAP,
    role_ordering: Ordering,
    is_implicit: bool,
) -> HashSet<CapabilityConstraint<CAP>> {
    let mut constraints = if is_implicit {
        HashSet::new()
    } else {
        HashSet::from([CapabilityConstraint::new(
            ConstraintDescription::Cardinality(Relates::get_default_cardinality_for_explicit(role_ordering)),
            source,
        )])
    };

    if let Some(default_distinct) = Relates::get_default_distinct(role_ordering) {
        constraints.insert(CapabilityConstraint::new(ConstraintDescription::Distinct(default_distinct), source));
    }

    constraints
}

pub(crate) fn type_get_constraints_closest_source<'a, T: KindAPI>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    constraints: impl IntoIterator<Item = &'a TypeConstraint<T>>,
) -> Option<T> {
    constraints
        .into_iter()
        .map(|constraint| constraint.source())
        .sorted_by(|lhs, rhs| {
            if lhs.is_subtype_transitive_of(snapshot, type_manager, *rhs).unwrap_or(false) {
                std::cmp::Ordering::Less
            } else if lhs.is_supertype_transitive_of(snapshot, type_manager, *rhs).unwrap_or(false) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .next()
}

typedb_error! {
    pub ConstraintError(component = "Constraint", prefix = "CNT") {
        CannotUnwrap(1, "Error getting mandatory constraint of type {type_}.", type_: &'static str),
        CorruptConstraintIsNotApplicableToValue(2, "Reached an invalid state: constraint {description} cannot be applied to value {value}.", description: ConstraintDescription, value: Value<'static>),
        IsNotNarrowedBy(3, "Constraint {first} is not narrowed by {second}.", first: ConstraintDescription, second: ConstraintDescription),
        ViolatedAbstract(4, "Constraint '@abstract' has been violated."),
        ViolatedCardinality(5, "Constraint '{cardinality}' has been violated: found {count} instances.", cardinality: AnnotationCardinality, count: u64),
        ViolatedRegex(6, "Constraint '{regex}' has been violated: value '{value}' does not match the regex.", regex: AnnotationRegex, value: Value<'static>),
        ViolatedRange(7, "Constraint '{range}' has been violated: value '{value}' does not match the range.", range: AnnotationRange, value: Value<'static>),
        ViolatedValues(8, "Constraint '{values}' has been violated: value '{value}' does not match the set of values.", values: AnnotationValues, value: Value<'static>),
        ViolatedUnique(9, "Constraint '@unique' has been violated: there is a conflict for value '{value}'.", value: Value<'static>),
        ViolatedDistinct(10, "Constraint '@distinct' has been violated: found {count} instances", count: u64),
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    iter,
    sync::Arc,
    vec,
};
use std::collections::HashMap;

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::OwnsInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::OwnerAPI,
};
use itertools::Itertools;
use concept::type_::{Capability, ObjectTypeAPI};
use concept::type_::attribute_type::AttributeType;
use concept::type_::object_type::ObjectType;
use concept::type_::type_manager::TypeManager;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, AsNarrowingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, OwnsToTupleFn, TuplePositions, TupleResult,
        },
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct OwnsExecutor {
    owns: ir::pattern::constraint::Owns<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<(AsHkt![ObjectType<'_>], AsHkt![AttributeType<'_>])>,
}

pub(super) type OwnsTupleIterator<I> =
    Map<TryFilter<I, Box<OwnsFilterFn>, (AsHkt![ObjectType<'_>], AsHkt![AttributeType<'_>]), ConceptReadError>, OwnsToTupleFn, AsHkt![TupleResult<'_>]>;

pub(super) type OwnsUnboundedSortedOwner = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<(ObjectType<'static>, AttributeType<'static>)>>>,
            fn((ObjectType<'static>, AttributeType<'static>)) -> Result<(ObjectType<'static>, AttributeType<'static>), ConceptReadError>,
        >,
        Result<(AsHkt![ObjectType<'_>], AsHkt![AttributeType<'_>]), ConceptReadError>,
    >,
>;
pub(super) type OwnsBoundedSortedAttribute = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<(ObjectType<'static>, AttributeType<'static>)>, fn((ObjectType<'static>, AttributeType<'static>)) -> Result<(ObjectType<'static>, AttributeType<'static>), ConceptReadError>>,
        Result<(AsHkt![ObjectType<'_>], AsHkt![AttributeType<'_>]), ConceptReadError>,
    >,
>;

pub(super) type OwnsFilterFn = FilterFn<(AsHkt![ObjectType<'_>], AsHkt![AttributeType<'_>])>;

pub(super) type OwnsVariableValueExtractor = for<'a> fn(&'a (ObjectType<'_>, AttributeType<'_>)) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: OwnsVariableValueExtractor =
    |(owner, _)| VariableValue::Type(Type::from(owner.clone().into_owned()));
pub(super) const EXTRACT_ATTRIBUTE: OwnsVariableValueExtractor =
    |(_, attribute)| VariableValue::Type(Type::Attribute(attribute.clone().into_owned()));

impl OwnsExecutor {
    pub(crate) fn new(
        owns: OwnsInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let attribute_types = owns.attribute_types().clone();
        let owner_attribute_types = owns.owner_attribute_types().clone();
        debug_assert!(attribute_types.len() > 0);

        let OwnsInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.owner(), owns.attribute(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(owner_attribute_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(attribute_types.clone())
            }
        };

        let owner = owns.owner().as_variable();
        let attribute = owns.attribute().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([owner, attribute]),
            _ => TuplePositions::Pair([attribute, owner]),
        };

        let checker = Checker::<(ObjectType<'_>, AttributeType<'_>)>::new(
            checks,
            [(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, OwnsVariableValueExtractor>>(),
        );

        Self {
            owns,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            owner_attribute_types,
            attribute_types,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<OwnsFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let owns: Vec<_> = self
                    .owner_attribute_types
                    .keys()
                    .map(|owner| self.get_owns_for_owner(snapshot, type_manager, owner.clone()))
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map(Ok as _);
                let as_tuples: OwnsUnboundedSortedOwner = AsNarrowingIterator::<_, Result<(ObjectType<'_>, AttributeType<'_>), _>>::new(iterator)
                    .try_filter::<_, OwnsFilterFn, (ObjectType<'_>, AttributeType<'_>), _>(filter_for_row)
                    .map(owns_to_tuple_owner_attribute);
                Ok(TupleIterator::OwnsUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let owner = type_from_row_or_annotations(self.owns.owner(), row, self.owner_attribute_types.keys());
                let type_manager = context.type_manager();
                let owns = self.get_owns_for_owner(snapshot, type_manager, owner)?;

                let iterator = owns.into_iter().sorted_by_key(|(owner, attribute)| (attribute.clone(), owner.clone())).map(Ok as _);
                let as_tuples: OwnsBoundedSortedAttribute =
                    AsNarrowingIterator::<_, Result<(ObjectType<'_>, AttributeType<'_>), _>>::new(iterator)
                        .try_filter::<_, OwnsFilterFn, (ObjectType<'_>, AttributeType<'_>), _>(filter_for_row)
                        .map(owns_to_tuple_attribute_owner);
                Ok(TupleIterator::OwnsBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn get_owns_for_owner(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owner: Type,
    ) -> Result<HashSet<(ObjectType<'static>, AttributeType<'static>)>, ConceptReadError> {
        let object_type = match owner {
            Type::Entity(entity) => entity.into_owned_object_type(),
            Type::Relation(relation) => relation.into_owned_object_type(),
            _ => unreachable!("owner types must be relation or entity types"),
        };

        Ok(object_type
            .get_owned_attribute_types(snapshot, type_manager)?
            .into_iter()
            .map(|attribute_type| (object_type.clone(), attribute_type))
            .collect())
    }
}

fn create_owns_filter_owner_attribute(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, attribute)) => match owner_attribute_types.get(&Type::from(owner.clone().into_owned())) {
            Some(attribute_types) => Ok(attribute_types.contains(&Type::Attribute(attribute.clone().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(attribute_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, attribute)) => Ok(attribute_types.contains(&Type::Attribute(attribute.clone().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}

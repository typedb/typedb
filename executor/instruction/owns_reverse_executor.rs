/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::type_::OwnsReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, object_type::ObjectType},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        owns_executor::{
            OwnsFilterFn, OwnsFilterMapFn, OwnsTupleIterator, OwnsVariableValueExtractor, EXTRACT_ATTRIBUTE,
            EXTRACT_OWNER,
        },
        plays_executor::PlaysExecutor,
        tuple::{owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct OwnsReverseExecutor {
    owns: ir::pattern::constraint::Owns<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<(ObjectType, AttributeType)>,
}

impl fmt::Debug for OwnsReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OwnsReverseExecutor")
    }
}

pub(super) type OwnsReverseUnboundedSortedAttribute = OwnsTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(ObjectType, AttributeType)>>>,
        fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
    >,
>;
pub(super) type OwnsReverseBoundedSortedOwner = OwnsTupleIterator<
    iter::Map<
        vec::IntoIter<(ObjectType, AttributeType)>,
        fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
    >,
>;

impl OwnsReverseExecutor {
    pub(crate) fn new(
        owns: OwnsReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let owner_types = owns.owner_types().clone();
        let attribute_owner_types = owns.attribute_owner_types().clone();
        debug_assert!(owner_types.len() > 0);

        let OwnsReverseInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.attribute(), owns.owner(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(owner_types.clone())
            }
        };

        let owner = owns.owner().as_variable();
        let attribute = owns.attribute().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([attribute, owner]),
            _ => TuplePositions::Pair([owner, attribute]),
        };

        let checker = Checker::<(ObjectType, AttributeType)>::new(
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
            attribute_owner_types,
            owner_types,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<OwnsFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let owns: Vec<_> = self
                    .attribute_owner_types
                    .keys()
                    .map(|attribute| {
                        let attribute_type = attribute.as_attribute_type();
                        attribute_type.get_owner_types(snapshot, type_manager).map(|res| {
                            res.to_owned().keys().map(|object_type| (*object_type, attribute_type)).collect()
                        })
                    })
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map(Ok as _);
                let as_tuples: OwnsReverseUnboundedSortedAttribute =
                    iterator.filter_map(filter_for_row).map(owns_to_tuple_attribute_owner as _);
                Ok(TupleIterator::OwnsReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                // is this ever relevant?
                return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
                    functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
                }));
            }

            BinaryIterateMode::BoundFrom => {
                let attribute_type =
                    type_from_row_or_annotations(self.owns.attribute(), row, self.attribute_owner_types.keys())
                        .as_attribute_type();
                let type_manager = context.type_manager();
                let owns = attribute_type
                    .get_owner_types(snapshot, type_manager)?
                    .to_owned()
                    .into_keys()
                    .map(|object_type| (object_type, attribute_type));

                let iterator = owns.sorted_by_key(|(owner, _)| *owner).map(Ok as _);
                let as_tuples: OwnsReverseBoundedSortedOwner =
                    iterator.filter_map(filter_for_row).map(owns_to_tuple_owner_attribute as _);

                Ok(TupleIterator::OwnsReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for OwnsReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.owns, &self.iterate_mode)
    }
}

fn create_owns_filter_owner_attribute(attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, attribute)) => match attribute_owner_types.get(&Type::Attribute(*attribute)) {
            Some(owner_types) => Ok(owner_types.contains(&Type::from(*owner))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(owner_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, _)) => Ok(owner_types.contains(&Type::from(*owner))),
        Err(err) => Err(err.clone()),
    })
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::SubInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, SubToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct SubExecutor {
    sub: ir::pattern::constraint::Sub<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<BTreeSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<(Type, Type)>,
}

impl fmt::Debug for SubExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubExecutor")
    }
}

pub(super) type SubTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<SubFilterMapFn>>, SubToTupleFn>;

pub(super) type SubUnboundedSortedSub = SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;
pub(super) type SubBoundedSortedSuper = SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;
pub(super) type SubFilterMapFn = FilterMapUnchangedFn<(Type, Type)>;

type SubVariableValueExtractor = fn(&(Type, Type)) -> VariableValue<'_>;
pub(super) const EXTRACT_SUB: SubVariableValueExtractor = |(sub, _)| VariableValue::Type(*sub);
pub(super) const EXTRACT_SUPER: SubVariableValueExtractor = |(_, sup)| VariableValue::Type(*sup);

impl SubExecutor {
    pub(crate) fn new(
        sub: SubInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let supertypes = sub.supertypes().clone();
        let sub_to_supertypes = sub.sub_to_supertypes().clone();
        debug_assert!(supertypes.len() > 0);

        let SubInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.subtype(), sub.supertype(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_sub_filter_sub_super(sub_to_supertypes.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_super(supertypes.clone())
            }
        };

        let subtype = sub.subtype().as_variable();
        let supertype = sub.supertype().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([subtype, supertype]),
            _ => TuplePositions::Pair([supertype, subtype]),
        };

        let checker = Checker::<(Type, Type)>::new(
            checks,
            [(subtype, EXTRACT_SUB), (supertype, EXTRACT_SUPER)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            sub,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            sub_to_supertypes,
            supertypes,
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
        let filter_for_row: Box<SubFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let sub_with_super = self
                    .sub_to_supertypes
                    .iter()
                    .flat_map(|(sub, supers)| supers.iter().map(|sup| Ok((*sub, *sup))))
                    .collect_vec();
                let as_tuples: SubUnboundedSortedSub =
                    sub_with_super.into_iter().filter_map(filter_for_row).map(sub_to_tuple_sub_super);
                Ok(TupleIterator::SubUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                // is this ever relevant?
                Err(Box::new(ConceptReadError::UnimplementedFunctionality {
                    functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
                }))
            }

            BinaryIterateMode::BoundFrom => {
                let subtype = type_from_row_or_annotations(self.sub.subtype(), row, self.sub_to_supertypes.keys());
                let Some(subtype) = subtype else { return Ok(TupleIterator::empty()) };
                let supertypes = self.sub_to_supertypes.get(&subtype).unwrap_or(const { &Vec::new() });
                let sub_with_super = supertypes.iter().map(|sup| Ok((subtype, *sup))).collect_vec(); // TODO cache this

                let as_tuples: SubBoundedSortedSuper =
                    sub_with_super.into_iter().filter_map(filter_for_row).map(sub_to_tuple_super_sub);
                Ok(TupleIterator::SubBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for SubExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.sub, &self.iterate_mode)
    }
}

fn create_sub_filter_sub_super(sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match sub_to_supertypes.get(sub) {
            Some(supertypes) => Ok(supertypes.contains(sup)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_super(supertypes: Arc<BTreeSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, sup)) => Ok(supertypes.contains(sup)),
        Err(err) => Err(err.clone()),
    })
}

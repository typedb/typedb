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

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::RelatesInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{relation_type::RelationType, role_type::RoleType, type_manager::TypeManager},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_reverse_executor::RelatesReverseExecutor,
        tuple::{relates_to_tuple_relation_role, relates_to_tuple_role_relation, RelatesToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct RelatesExecutor {
    relates: ir::pattern::constraint::Relates<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<RelatesFilterFn>,
    checker: Checker<(RelationType, RoleType)>,
}

impl fmt::Debug for RelatesExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelatesExecutor")
    }
}

pub(super) type RelatesTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<RelatesFilterMapFn>>, RelatesToTupleFn>;

pub(super) type RelatesUnboundedSortedRelation = RelatesTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(RelationType, RoleType)>>>,
        fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
    >,
>;
pub(super) type RelatesBoundedSortedRole = RelatesTupleIterator<
    iter::Map<
        vec::IntoIter<(RelationType, RoleType)>,
        fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
    >,
>;

pub(super) type RelatesFilterFn = FilterFn<(RelationType, RoleType)>;
pub(super) type RelatesFilterMapFn = FilterMapUnchangedFn<(RelationType, RoleType)>;

pub(super) type RelatesVariableValueExtractor = for<'a> fn(&'a (RelationType, RoleType)) -> VariableValue<'a>;
pub(super) const EXTRACT_RELATION: RelatesVariableValueExtractor =
    |(relation, _)| VariableValue::Type(Type::Relation(*relation));
pub(super) const EXTRACT_ROLE: RelatesVariableValueExtractor = |(_, role)| VariableValue::Type(Type::RoleType(*role));

impl RelatesExecutor {
    pub(crate) fn new(
        relates: RelatesInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let role_types = relates.role_types().clone();
        let relation_role_types = relates.relation_role_types().clone();
        debug_assert!(role_types.len() > 0);

        let RelatesInstruction { relates, checks, .. } = relates;

        let iterate_mode = BinaryIterateMode::new(relates.relation(), relates.role_type(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_relates_filter_relation_role_type(relation_role_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_relates_filter_role_type(role_types.clone())
            }
        };

        let relation = relates.relation().as_variable();
        let role_type = relates.role_type().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([relation, role_type]),
            _ => TuplePositions::Pair([role_type, relation]),
        };

        let checker = Checker::<(RelationType, RoleType)>::new(
            checks,
            [(relation, EXTRACT_RELATION), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, RelatesVariableValueExtractor>>(),
        );

        Self {
            relates,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            relation_role_types,
            role_types,
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
        let filter_for_row: Box<RelatesFilterMapFn> = Box::new(move |item| match filter(&item) {
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
                let relates: Vec<_> = self
                    .relation_role_types
                    .keys()
                    .map(|relation| self.get_relates_for_relation(snapshot, type_manager, *relation))
                    .try_collect()?;
                let iterator = relates.into_iter().flatten().map(Ok as _);
                let as_tuples: RelatesUnboundedSortedRelation =
                    iterator.filter_map(filter_for_row).map(relates_to_tuple_relation_role as _);
                Ok(TupleIterator::RelatesUnbounded(SortedTupleIterator::new(
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
                let relation =
                    type_from_row_or_annotations(self.relates.relation(), row, self.relation_role_types.keys());
                let type_manager = context.type_manager();
                let relates = self.get_relates_for_relation(snapshot, type_manager, relation)?;

                let iterator =
                    relates.iter().cloned().sorted_by_key(|(relation, role)| (*role, *relation)).map(Ok as _);
                let as_tuples: RelatesBoundedSortedRole =
                    iterator.filter_map(filter_for_row).map(relates_to_tuple_role_relation as _);
                Ok(TupleIterator::RelatesBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn get_relates_for_relation(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation: Type,
    ) -> Result<BTreeSet<(RelationType, RoleType)>, Box<ConceptReadError>> {
        let relation_type = relation.as_relation_type();

        Ok(relation_type
            .get_related_role_types(snapshot, type_manager)?
            .into_iter()
            .map(|role_type| (relation_type, role_type))
            .collect())
    }
}

impl fmt::Display for RelatesExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.relates, &self.iterate_mode)
    }
}

fn create_relates_filter_relation_role_type(
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, role)) => match relation_role_types.get(&Type::from(*relation)) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(*role))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role_type(role_types: Arc<BTreeSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, role)) => Ok(role_types.contains(&Type::RoleType(*role))),
        Err(err) => Err(err.clone()),
    })
}

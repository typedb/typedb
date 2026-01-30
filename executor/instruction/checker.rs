/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{Bound, HashMap},
    marker::PhantomData,
    sync::Arc,
};

use answer::{variable_value::VariableValue, Thing, Type};
use bytes::byte_array::ByteArray;
use compiler::{
    executable::match_::instructions::{CheckInstruction, CheckVertex},
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager, ThingAPI},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::{
    graph::thing::THING_VERTEX_MAX_LENGTH,
    value::{value::Value, ValueEncodable},
    AsBytes,
};
use error::unimplemented_feature;
use ir::{
    pattern::constraint::{Comparator, IsaKind, SubKind},
    pipeline::ParameterRegistry,
};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use unicase::UniCase;

use crate::{instruction::FilterFn, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};

#[derive(Debug)]
pub(crate) struct Checker<T: 'static> {
    extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    pub checks: Vec<CheckInstruction<ExecutorVariable>>,
    _phantom_data: PhantomData<T>,
}

type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a T) -> VariableValue<'a>>;

macro_rules! unwrap_or_result_false {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return Ok(false) };
        x
    }};
}

macro_rules! unwrap_or_return_false {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return false };
        x
    }};
}

impl<T> Checker<T> {
    pub(crate) fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        Self { extractors, checks, _phantom_data: PhantomData }
    }

    pub(crate) fn value_range_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: Option<MaybeOwnedRow<'_>>,
        target_variable: ExecutorVariable,
        storage_counters: StorageCounters,
    ) -> Result<(Bound<Value<'_>>, Bound<Value<'_>>), Box<ConceptReadError>> {
        fn intersect<'a>(
            (a_min, a_max): (Bound<Value<'a>>, Bound<Value<'a>>),
            (b_min, b_max): (Bound<Value<'a>>, Bound<Value<'a>>),
        ) -> (Bound<Value<'a>>, Bound<Value<'a>>) {
            let select_a_min = match (&a_min, &b_min) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a >= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
                (Bound::Included(a), Bound::Included(b)) => a >= b,
                (Bound::Included(a), Bound::Excluded(b)) => a > b,
                _ => false,
            };
            let select_a_max = match (&a_max, &b_max) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a <= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a <= b,
                (Bound::Included(a), Bound::Included(b)) => a <= b,
                (Bound::Included(a), Bound::Excluded(b)) => a < b,
                _ => false,
            };
            (if select_a_min { a_min } else { b_min }, if select_a_max { a_max } else { b_max })
        }

        let mut range = (Bound::Unbounded, Bound::Unbounded);
        for i in 0..self.checks.len() {
            let check = &self.checks[i];
            match check {
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    if lhs.as_variable() == Some(target_variable) {
                        let rhs_variable_value = get_vertex_value(rhs, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value)),
                                Comparator::Less => (Bound::Unbounded, Bound::Excluded(rhs_value)),
                                Comparator::LessOrEqual => (Bound::Unbounded, Bound::Included(rhs_value)),
                                Comparator::Greater => (Bound::Excluded(rhs_value), Bound::Unbounded),
                                Comparator::GreaterOrEqual => (Bound::Included(rhs_value), Bound::Unbounded),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    } else {
                        debug_assert!(
                            rhs.as_variable().expect("RHS of comparison must be a variable") == target_variable
                        );
                        let lhs_variable_value = get_vertex_value(lhs, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value)),
                                Comparator::Less => (Bound::Excluded(lhs_value), Bound::Unbounded),
                                Comparator::LessOrEqual => (Bound::Included(lhs_value), Bound::Unbounded),
                                Comparator::Greater => (Bound::Unbounded, Bound::Excluded(lhs_value)),
                                Comparator::GreaterOrEqual => (Bound::Unbounded, Bound::Included(lhs_value)),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    }
                }
                CheckInstruction::Is { lhs, rhs } => {
                    if *lhs == target_variable {
                        let rhs_as_vertex = CheckVertex::Variable(*rhs);
                        let rhs_variable_value = get_vertex_value(&rhs_as_vertex, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value));
                            range = intersect(range, comp_range);
                        }
                    } else {
                        let lhs_as_vertex = CheckVertex::Variable(*lhs);
                        let lhs_variable_value = get_vertex_value(&lhs_as_vertex, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value));
                            range = intersect(range, comp_range);
                        }
                    }
                }
                _ => (),
            }
        }
        let range = (range.0.map(|value| value.into_owned()), range.1.map(|value| value.into_owned()));
        Ok(range)
    }

    fn read_value<'a>(
        snapshot: &'a impl ReadableSnapshot,
        thing_manager: &'a ThingManager,
        variable_value: &'a VariableValue<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Value<'static>>, Box<ConceptReadError>> {
        // TODO: is there a way to do this without cloning the value?
        match variable_value {
            VariableValue::Thing(Thing::Attribute(attribute)) => {
                let value = attribute.get_value(snapshot, thing_manager, storage_counters)?;
                Ok(Some(value.into_owned()))
            }
            VariableValue::Value(value) => {
                let value = value.as_reference();
                Ok(Some(value.into_owned()))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn filter(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: T,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        for check in &self.checks {
            let passes = match check {
                CheckInstruction::Iid { var, iid } => self.filter_iid(context, row, *var, &source, iid),
                CheckInstruction::TypeList { type_var, types } => {
                    self.filter_type_list(context, row, *type_var, &source, types)
                }
                CheckInstruction::ThingTypeList { thing_var, types } => {
                    self.filter_thing_type_list(context, row, *thing_var, &source, types)
                }
                CheckInstruction::Sub { sub_kind, subtype, supertype } => {
                    self.filter_sub(context, row, *sub_kind, &source, subtype, supertype)?
                }
                CheckInstruction::Owns { owner, attribute } => {
                    self.filter_owns(context, row, &source, owner, attribute)?
                }
                CheckInstruction::Relates { relation, role_type } => {
                    self.filter_relates(context, row, &source, relation, role_type)?
                }
                CheckInstruction::Plays { player, role_type } => {
                    self.filter_plays(context, row, &source, player, role_type)?
                }
                CheckInstruction::Isa { isa_kind, ref type_, ref thing } => {
                    self.filter_isa(context, row, &source, *isa_kind, type_, thing)?
                }
                CheckInstruction::Has { owner, attribute } => {
                    self.filter_has(context, row, &source, owner, attribute, storage_counters.clone())?
                }
                CheckInstruction::Links { relation, player, role } => {
                    self.filter_links(context, row, &source, relation, player, role, storage_counters.clone())?
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => self
                    .filter_indexed_relation(
                        context,
                        row,
                        &source,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    )?,
                CheckInstruction::Is { lhs, rhs } => self.filter_is(row, &source, *lhs, *rhs),
                CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    self.filter_links_dedup(row, &source, *role1, *player1, *role2, *player2)
                }
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    self.filter_comparison(context, row, &source, lhs, rhs, *comparator, storage_counters.clone())?
                }
                CheckInstruction::NotNone { variables } => Self::filter_not_none(row, variables),
                CheckInstruction::Unsatisfiable => false,
            };
            if !passes {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn filter_fn_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Box<FilterFn<T>> {
        let mut filters: Vec<Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>>> =
            Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            let filter = match check {
                CheckInstruction::Iid { var, iid } => self.filter_iid_fn(context, row, *var, iid),
                &CheckInstruction::TypeList { type_var, ref types } => {
                    self.filter_type_list_fn(context, row, type_var, types)
                }
                &CheckInstruction::ThingTypeList { thing_var, ref types } => {
                    self.filter_thing_type_list_fn(context, row, thing_var, types)
                }
                &CheckInstruction::Sub { sub_kind, ref subtype, ref supertype } => {
                    self.filter_sub_fn(context, row, sub_kind, subtype, supertype)
                }
                CheckInstruction::Owns { owner, attribute } => self.filter_owns_fn(context, row, owner, attribute),
                CheckInstruction::Relates { relation, role_type } => {
                    self.filter_relates_fn(context, row, relation, role_type)
                }
                CheckInstruction::Plays { player, role_type } => self.filter_plays_fn(context, row, player, role_type),
                &CheckInstruction::Isa { isa_kind, ref type_, ref thing } => {
                    self.filter_isa_fn(context, row, isa_kind, type_, thing)
                }
                CheckInstruction::Has { owner, attribute } => {
                    self.filter_has_fn(context, row, owner, attribute, storage_counters.clone())
                }
                CheckInstruction::Links { relation, player, role } => {
                    self.filter_links_fn(context, row, relation, player, role, storage_counters.clone())
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => self
                    .filter_indexed_relation_fn(
                        context,
                        row,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    ),
                &CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    self.filter_links_dedup_fn(row, role1, player1, role2, player2)
                }
                CheckInstruction::NotNone { variables } => {
                    todo!()
                    // self.filter_not(context, row, check)
                }
                &CheckInstruction::Is { lhs, rhs } => self.filter_is_fn(row, lhs, rhs),
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    self.filter_comparison_fn(context, row, lhs, rhs, *comparator, storage_counters.clone())
                }
                CheckInstruction::Unsatisfiable => Box::new(|_: &T| Ok(false)),
            };
            filters.push(filter);
        }

        Box::new(move |res| {
            let Ok(value) = res else { return Ok(true) };
            for filter in &filters {
                if !filter(value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    fn filter_iid_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        var: ExecutorVariable,
        iid: &ir::pattern::ParameterID,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let var: BoxExtractor<T> = match self.extractors.get(&var) {
            Some(&function) => Box::new(function),
            None => make_const_extractor(&CheckVertex::Variable(var), row, context),
        };
        let iid = context.parameters().iid(iid).unwrap().clone();
        Box::new(move |value: &T| Ok(Self::check_iid(&iid, var(value))))
    }

    fn filter_iid(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        var: ExecutorVariable,
        source: &T,
        iid: &ir::pattern::ParameterID,
    ) -> bool {
        let extracted = match self.extractors.get(&var) {
            Some(function) => function(source),
            None => get_vertex_value(&CheckVertex::Variable(var), Some(row), &context.parameters),
        };
        let iid = context.parameters().iid(iid).unwrap();
        Self::check_iid(iid, extracted)
    }

    fn check_iid(iid: &ByteArray<{ THING_VERTEX_MAX_LENGTH }>, value: VariableValue<'_>) -> bool {
        match value {
            VariableValue::Thing(thing) => match thing {
                Thing::Entity(entity) => *iid == *entity.vertex().to_bytes(),
                Thing::Relation(relation) => *iid == *relation.vertex().to_bytes(),
                Thing::Attribute(attribute) => *iid == *attribute.vertex().to_bytes(),
            },
            VariableValue::None => false,
            VariableValue::Type(_) => false,
            VariableValue::Value(_) => false, // or unreachable?
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        }
    }

    fn filter_type_list_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        type_var: ExecutorVariable,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_type_extractor = self.extractors.get(&type_var);
        let type_: BoxExtractor<T> = match maybe_type_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(&CheckVertex::Variable(type_var), row, context),
        };
        let types = types.clone();
        Box::new(move |value: &T| Ok(types.contains(&unwrap_or_result_false!(type_(value) => Type))))
    }

    fn filter_type_list(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        type_var: ExecutorVariable,
        source: &T,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> bool {
        let extracted = match self.extractors.get(&type_var) {
            Some(function) => function(source),
            None => get_vertex_value(&CheckVertex::Variable(type_var), Some(row), &context.parameters),
        };
        types.contains(&unwrap_or_return_false!(extracted => Type))
    }

    fn filter_thing_type_list_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        thing_var: ExecutorVariable,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_type_extractor = self.extractors.get(&thing_var);
        let thing: BoxExtractor<T> = match maybe_type_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(&CheckVertex::Variable(thing_var), row, context),
        };
        let types = types.clone();
        Box::new(move |value: &T| Ok(types.contains(&unwrap_or_result_false!(thing(value) => Thing).type_())))
    }

    fn filter_thing_type_list(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        thing_var: ExecutorVariable,
        source: &T,
        types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
    ) -> bool {
        let extracted = match self.extractors.get(&thing_var) {
            Some(function) => function(source),
            None => get_vertex_value(&CheckVertex::Variable(thing_var), Some(row), &context.parameters),
        };
        types.contains(&unwrap_or_return_false!(extracted => Thing).type_())
    }

    fn filter_sub_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        sub_kind: SubKind,
        subtype: &CheckVertex<ExecutorVariable>,
        supertype: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let maybe_subtype_extractor = subtype.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_supertype_extractor = supertype.as_variable().and_then(|var| self.extractors.get(&var));
        let subtype: BoxExtractor<T> = match maybe_subtype_extractor {
            Some(&subtype) => Box::new(subtype),
            None => make_const_extractor(subtype, row, context),
        };
        let supertype: BoxExtractor<T> = match maybe_supertype_extractor {
            Some(&supertype) => Box::new(supertype),
            None => make_const_extractor(supertype, row, context),
        };
        Box::new(move |source: &T| {
            let subtype = unwrap_or_result_false!(subtype(source) => Type);
            let supertype = unwrap_or_result_false!(supertype(source) => Type);
            Self::check_sub(&*snapshot, &*thing_manager, sub_kind, subtype, supertype)
        })
    }

    fn filter_sub(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        sub_kind: SubKind,
        source: &T,
        subtype: &CheckVertex<ExecutorVariable>,
        supertype: &CheckVertex<ExecutorVariable>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let subtype = match subtype.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(subtype, Some(row), &context.parameters),
        };
        let supertype = match supertype.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(supertype, Some(row), &context.parameters),
        };
        Self::check_sub(
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            sub_kind,
            unwrap_or_result_false!(subtype => Type),
            unwrap_or_result_false!(supertype => Type),
        )
    }

    fn check_sub(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        sub_kind: SubKind,
        subtype: Type,
        supertype: Type,
    ) -> Result<bool, Box<ConceptReadError>> {
        match sub_kind {
            SubKind::Subtype => subtype.is_transitive_subtype_of(supertype, &*snapshot, thing_manager.type_manager()),
            SubKind::Exact => subtype.is_direct_subtype_of(supertype, &*snapshot, thing_manager.type_manager()),
        }
    }

    fn filter_owns_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
        let owner: BoxExtractor<T> = match maybe_owner_extractor {
            Some(&owner) => Box::new(owner),
            None => make_const_extractor(owner, row, context),
        };
        let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
            Some(&attribute) => Box::new(attribute),
            None => make_const_extractor(attribute, row, context),
        };
        Box::new(move |value: &T| {
            let owner = unwrap_or_result_false!(owner(value) => Type).as_object_type();
            let attribute = unwrap_or_result_false!(attribute(value) => Type).as_attribute_type();
            owner.get_owns_attribute(&*snapshot, thing_manager.type_manager(), attribute).map(|owns| owns.is_some())
        })
    }

    fn filter_owns(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let owner = match owner.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(&function) => function(source),
            None => get_vertex_value(owner, Some(row), &context.parameters),
        };
        let attribute = match attribute.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(&function) => function(source),
            None => get_vertex_value(attribute, Some(row), &context.parameters),
        };
        let owner = unwrap_or_result_false!(owner => Type).as_object_type();
        let attribute = unwrap_or_result_false!(attribute => Type).as_attribute_type();
        owner
            .get_owns_attribute(context.snapshot.as_ref(), context.thing_manager.clone().type_manager(), attribute)
            .map(|owns| owns.is_some())
    }

    fn filter_relates_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let relation: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
            Some(&role_type) => Box::new(role_type),
            None => make_const_extractor(role_type, row, context),
        };
        Box::new(move |value: &T| {
            let relation_type = unwrap_or_result_false!(relation(value) => Type).as_relation_type();
            let role_type = unwrap_or_result_false!(role_type(value) => Type).as_role_type();
            relation_type
                .get_relates_role(&*snapshot, thing_manager.type_manager(), role_type)
                .map(|relates| relates.is_some())
        })
    }

    fn filter_relates(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        relation: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let relation = match relation.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(relation, Some(row), &context.parameters),
        };
        let role_type = match role_type.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(role_type, Some(row), &context.parameters),
        };
        let relation_type = unwrap_or_result_false!(relation => Type).as_relation_type();
        let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
        relation_type
            .get_relates_role(context.snapshot.as_ref(), context.thing_manager.type_manager(), role_type)
            .map(|relates| relates.is_some())
    }

    fn filter_plays_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        player: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_type_extractor = role_type.as_variable().and_then(|var| self.extractors.get(&var));
        let player: BoxExtractor<T> = match maybe_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(player, row, context),
        };
        let role_type: BoxExtractor<T> = match maybe_role_type_extractor {
            Some(&role_type) => Box::new(role_type),
            None => make_const_extractor(role_type, row, context),
        };
        Box::new({
            move |value: &T| {
                let object_type = unwrap_or_result_false!(player(value) => Type).as_object_type();
                let role_type = unwrap_or_result_false!(role_type(value) => Type).as_role_type();
                object_type
                    .get_plays_role(&*snapshot, thing_manager.type_manager(), role_type)
                    .map(|plays| plays.is_some())
            }
        })
    }

    fn filter_plays(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        player: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let player = match player.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(player, Some(row), &context.parameters),
        };
        let role_type = match role_type.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(&function) => function(source),
            None => get_vertex_value(role_type, Some(row), &context.parameters),
        };
        let object_type = unwrap_or_result_false!(player => Type).as_object_type();
        let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
        object_type
            .get_plays_role(context.snapshot.as_ref(), context.thing_manager.type_manager(), role_type)
            .map(|plays| plays.is_some())
    }

    fn filter_isa_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        isa_kind: IsaKind,
        type_: &CheckVertex<ExecutorVariable>,
        thing: &CheckVertex<ExecutorVariable>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_thing_extractor = thing.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_type_extractor = type_.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let thing: BoxExtractor<T> = match maybe_thing_extractor {
            Some(&thing) => Box::new(thing),
            None => make_const_extractor(thing, row, context),
        };
        let type_: BoxExtractor<T> = match maybe_type_extractor {
            Some(&type_) => Box::new(type_),
            None => make_const_extractor(type_, row, context),
        };
        Box::new({
            move |value: &T| {
                let actual = unwrap_or_result_false!(thing(value) => Thing).type_();
                let expected = unwrap_or_result_false!(type_(value) => Type);
                if isa_kind == IsaKind::Exact {
                    Ok(actual == expected)
                } else {
                    actual.is_transitive_subtype_of(expected, &*snapshot, thing_manager.type_manager())
                }
            }
        })
    }

    fn filter_isa(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        isa_kind: IsaKind,
        type_: &CheckVertex<ExecutorVariable>,
        thing: &CheckVertex<ExecutorVariable>,
    ) -> Result<bool, Box<ConceptReadError>> {
        let thing = match thing.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(thing, Some(row), &context.parameters),
        };
        let type_ = match type_.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(type_, Some(row), &context.parameters),
        };
        let actual = unwrap_or_result_false!(thing => Thing).type_();
        let expected = unwrap_or_result_false!(type_ => Type);
        if isa_kind == IsaKind::Exact {
            Ok(actual == expected)
        } else {
            actual.is_transitive_subtype_of(expected, context.snapshot.as_ref(), context.thing_manager.type_manager())
        }
    }

    fn filter_has_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_owner_extractor = owner.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_attribute_extractor = attribute.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let owner: BoxExtractor<T> = match maybe_owner_extractor {
            Some(&owner) => Box::new(owner),
            None => make_const_extractor(owner, row, context),
        };
        let attribute: BoxExtractor<T> = match maybe_attribute_extractor {
            Some(&attribute) => Box::new(attribute),
            None => make_const_extractor(attribute, row, context),
        };
        Box::new({
            move |value: &T| {
                let owner = unwrap_or_result_false!(owner(value) => Thing).as_object();
                let attribute = attribute(value);
                let attribute = unwrap_or_result_false!(&attribute => Thing).as_attribute();
                owner.has_attribute(&*snapshot, &thing_manager, attribute, storage_counters.clone())
            }
        })
    }

    fn filter_has(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let owner = match owner.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(owner, Some(row), &context.parameters),
        };
        let attribute = match attribute.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(attribute, Some(row), &context.parameters),
        };
        let owner = unwrap_or_result_false!(&owner => Thing).as_object();
        let attribute = unwrap_or_result_false!(&attribute => Thing).as_attribute();
        owner.has_attribute(
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            attribute,
            storage_counters.clone(),
        )
    }

    fn filter_links_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        player: &CheckVertex<ExecutorVariable>,
        role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_player_extractor = player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_role_extractor = role.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let relation: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let player: BoxExtractor<T> = match maybe_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(player, row, context),
        };
        let role: BoxExtractor<T> = match maybe_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(role, row, context),
        };
        Box::new({
            move |value: &T| {
                let relation = unwrap_or_result_false!(relation(value) => Thing).as_relation();
                let player = unwrap_or_result_false!(player(value) => Thing).as_object();
                let role = unwrap_or_result_false!(role(value) => Type).as_role_type();
                relation.has_role_player(&*snapshot, &thing_manager, player, role, storage_counters.clone())
            }
        })
    }

    fn filter_links(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        relation: &CheckVertex<ExecutorVariable>,
        player: &CheckVertex<ExecutorVariable>,
        role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let relation = match relation.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(relation, Some(row), &context.parameters),
        };
        let player = match player.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(player, Some(row), &context.parameters),
        };
        let role = match role.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(role, Some(row), &context.parameters),
        };
        let relation = unwrap_or_result_false!(relation => Thing).as_relation();
        let player = unwrap_or_result_false!(player => Thing).as_object();
        let role = unwrap_or_result_false!(role => Type).as_role_type();
        relation.has_role_player(
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            player,
            role,
            storage_counters.clone(),
        )
    }

    fn filter_indexed_relation_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        start_player: &CheckVertex<ExecutorVariable>,
        end_player: &CheckVertex<ExecutorVariable>,
        relation: &CheckVertex<ExecutorVariable>,
        start_role: &CheckVertex<ExecutorVariable>,
        end_role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_start_player_extractor = start_player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_end_player_extractor = end_player.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_relation_extractor = relation.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_start_role_extractor = start_role.as_variable().and_then(|var| self.extractors.get(&var));
        let maybe_end_role_extractor = end_role.as_variable().and_then(|var| self.extractors.get(&var));
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let start_player_extractor: BoxExtractor<T> = match maybe_start_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(start_player, row, context),
        };
        let end_player_extractor: BoxExtractor<T> = match maybe_end_player_extractor {
            Some(&player) => Box::new(player),
            None => make_const_extractor(end_player, row, context),
        };
        let relation_extractor: BoxExtractor<T> = match maybe_relation_extractor {
            Some(&relation) => Box::new(relation),
            None => make_const_extractor(relation, row, context),
        };
        let start_role_extractor: BoxExtractor<T> = match maybe_start_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(start_role, row, context),
        };
        let end_role_extractor: BoxExtractor<T> = match maybe_end_role_extractor {
            Some(&role) => Box::new(role),
            None => make_const_extractor(end_role, row, context),
        };
        Box::new({
            move |value: &T| {
                let object = unwrap_or_result_false!(start_player_extractor(value) => Thing).as_object();
                let end_player = unwrap_or_result_false!(end_player_extractor(value) => Thing).as_object();
                let relation = unwrap_or_result_false!(relation_extractor(value) => Thing).as_relation();
                let start_role = unwrap_or_result_false!(start_role_extractor(value) => Type).as_role_type();
                let end_role = unwrap_or_result_false!(end_role_extractor(value) => Type).as_role_type();
                object.has_indexed_relation_player(
                    &*snapshot,
                    &thing_manager,
                    end_player,
                    relation,
                    start_role,
                    end_role,
                    storage_counters.clone(),
                )
            }
        })
    }

    fn filter_indexed_relation(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        start_player: &CheckVertex<ExecutorVariable>,
        end_player: &CheckVertex<ExecutorVariable>,
        relation: &CheckVertex<ExecutorVariable>,
        start_role: &CheckVertex<ExecutorVariable>,
        end_role: &CheckVertex<ExecutorVariable>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let start_player_extractor = match start_player.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(start_player, Some(row), &context.parameters),
        };
        let end_player_extractor = match end_player.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(end_player, Some(row), &context.parameters),
        };
        let relation_extractor = match relation.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(relation, Some(row), &context.parameters),
        };
        let start_role_extractor = match start_role.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(start_role, Some(row), &context.parameters),
        };
        let end_role_extractor = match end_role.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(end_role, Some(row), &context.parameters),
        };
        let object = unwrap_or_result_false!(start_player_extractor => Thing).as_object();
        let end_player = unwrap_or_result_false!(end_player_extractor => Thing).as_object();
        let relation = unwrap_or_result_false!(relation_extractor => Thing).as_relation();
        let start_role = unwrap_or_result_false!(start_role_extractor => Type).as_role_type();
        let end_role = unwrap_or_result_false!(end_role_extractor => Type).as_role_type();
        object.has_indexed_relation_player(
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            end_player,
            relation,
            start_role,
            end_role,
            storage_counters.clone(),
        )
    }

    fn filter_is_fn(
        &self,
        row: &MaybeOwnedRow<'_>,
        lhs: ExecutorVariable,
        rhs: ExecutorVariable,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_lhs_extractor = self.extractors.get(&lhs);
        let lhs: BoxExtractor<T> = match maybe_lhs_extractor {
            Some(&lhs) => Box::new(lhs),
            None => {
                let ExecutorVariable::RowPosition(pos) = lhs else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_rhs_extractor = self.extractors.get(&rhs);
        let rhs: BoxExtractor<T> = match maybe_rhs_extractor {
            Some(&rhs) => Box::new(rhs),
            None => {
                let ExecutorVariable::RowPosition(pos) = rhs else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        // NOTE: Empty is Empty matches
        Box::new(move |value: &T| Ok(lhs(value) == rhs(value)))
    }

    fn filter_is(&self, row: &MaybeOwnedRow<'_>, source: &T, lhs: ExecutorVariable, rhs: ExecutorVariable) -> bool {
        let lhs = match self.extractors.get(&lhs) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = lhs else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        let rhs = match self.extractors.get(&rhs) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = rhs else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        lhs == rhs
    }

    fn filter_links_dedup_fn(
        &self,
        row: &MaybeOwnedRow<'_>,
        role1: ExecutorVariable,
        player1: ExecutorVariable,
        role2: ExecutorVariable,
        player2: ExecutorVariable,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_role1_extractor = self.extractors.get(&role1);
        let role1: BoxExtractor<T> = match maybe_role1_extractor {
            Some(&role1) => Box::new(role1),
            None => {
                let ExecutorVariable::RowPosition(pos) = role1 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_player1_extractor = self.extractors.get(&player1);
        let player1: BoxExtractor<T> = match maybe_player1_extractor {
            Some(&player1) => Box::new(player1),
            None => {
                let ExecutorVariable::RowPosition(pos) = player1 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_role2_extractor = self.extractors.get(&role2);
        let role2: BoxExtractor<T> = match maybe_role2_extractor {
            Some(&role2) => Box::new(role2),
            None => {
                let ExecutorVariable::RowPosition(pos) = role2 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        let maybe_player2_extractor = self.extractors.get(&player2);
        let player2: BoxExtractor<T> = match maybe_player2_extractor {
            Some(&player2) => Box::new(player2),
            None => {
                let ExecutorVariable::RowPosition(pos) = player2 else { unreachable!() };
                let value = row.get(pos).as_reference().into_owned();
                Box::new(move |_| value.clone())
            }
        };
        Box::new(move |value: &T| Ok(!(role1(value) == role2(value) && player1(value) == player2(value))))
    }

    fn filter_links_dedup(
        &self,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        role1: ExecutorVariable,
        player1: ExecutorVariable,
        role2: ExecutorVariable,
        player2: ExecutorVariable,
    ) -> bool {
        let role1 = match self.extractors.get(&role1) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = role1 else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        let player1 = match self.extractors.get(&player1) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = player1 else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        let role2 = match self.extractors.get(&role2) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = role2 else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        let player2 = match self.extractors.get(&player2) {
            Some(function) => function(source),
            None => {
                let ExecutorVariable::RowPosition(pos) = player2 else { unreachable!() };
                row.get(pos).as_reference()
            }
        };
        !(role1 == role2 && player1 == player2)
    }

    fn filter_not_none_fn(
        row: &MaybeOwnedRow<'_>,
        variables: Arc<Vec<ExecutorVariable>>,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let values: Vec<_> = variables
            .iter()
            .map(|var| {
                let ExecutorVariable::RowPosition(pos) = var else { unreachable!() };
                row.get(*pos).clone().into_owned()
            })
            .collect();
        Box::new(move |_value: &T| Ok(values.iter().all(|value| !value.is_none())))
    }

    fn filter_not_none(row: &MaybeOwnedRow<'_>, variables: &[ExecutorVariable]) -> bool {
        variables.iter().all(|var| {
            let ExecutorVariable::RowPosition(pos) = var else { unreachable!() };
            let value = row.get(*pos);
            !value.is_none()
        })
    }

    fn filter_comparison_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        lhs: &CheckVertex<ExecutorVariable>,
        rhs: &CheckVertex<ExecutorVariable>,
        comparator: Comparator,
        storage_counters: StorageCounters,
    ) -> Box<dyn Fn(&T) -> Result<bool, Box<ConceptReadError>>> {
        let maybe_lhs_extractor = lhs.as_variable().and_then(|var| self.extractors.get(&var));
        let lhs: BoxExtractor<T> = match maybe_lhs_extractor {
            Some(&lhs) => Box::new(lhs),
            None => make_const_extractor(lhs, row, context),
        };
        let rhs = match rhs {
            &CheckVertex::Variable(ExecutorVariable::RowPosition(pos)) => row.get(pos).as_reference(),
            &CheckVertex::Variable(_) => unreachable!(),
            CheckVertex::Parameter(param) => {
                VariableValue::Value(context.parameters().value_unchecked(param).as_reference())
            }
            CheckVertex::Type(_) => unreachable!(),
        };
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let rhs = match rhs {
            VariableValue::Thing(Thing::Attribute(attr)) => {
                attr.get_value(&*snapshot, &thing_manager, storage_counters.clone()).map(Value::into_owned)
            }
            VariableValue::Value(value) => Ok(value.into_owned()),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
            VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
        };
        Box::new(move |value: &T| {
            // NOTE: Empty <op> Empty never matches
            let lhs = lhs(value);
            let lhs = match lhs {
                VariableValue::Thing(Thing::Attribute(attr)) => {
                    attr.get_value(&*snapshot, &thing_manager, storage_counters.clone())?.into_owned()
                }
                VariableValue::Value(value) => value,
                VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
                VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
            };
            let rhs = rhs.clone()?;
            if rhs.value_type().is_trivially_castable_to(lhs.value_type().category()) {
                Ok(Self::cmp_values_fn(&comparator)(&lhs, &rhs.cast(lhs.value_type().category()).unwrap()))
            } else if lhs.value_type().is_trivially_castable_to(rhs.value_type().category()) {
                Ok(Self::cmp_values_fn(&comparator)(&lhs.cast(rhs.value_type().category()).unwrap(), &rhs))
            } else {
                Ok(false)
            }
        })
    }

    fn filter_comparison(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        source: &T,
        lhs: &CheckVertex<ExecutorVariable>,
        rhs: &CheckVertex<ExecutorVariable>,
        comparator: Comparator,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        let lhs = match lhs.as_variable().and_then(|var| self.extractors.get(&var)) {
            Some(function) => function(source),
            None => get_vertex_value(lhs, Some(row), &context.parameters),
        };
        let rhs = match rhs {
            &CheckVertex::Variable(ExecutorVariable::RowPosition(pos)) => row.get(pos).as_reference(),
            &CheckVertex::Variable(_) => unreachable!(),
            CheckVertex::Parameter(param) => {
                VariableValue::Value(context.parameters().value_unchecked(param).as_reference())
            }
            CheckVertex::Type(_) => unreachable!(),
        };
        let rhs = match &rhs {
            VariableValue::Thing(Thing::Attribute(attr)) => {
                attr.get_value(context.snapshot.as_ref(), context.thing_manager.as_ref(), storage_counters.clone())?
            }
            VariableValue::Value(value) => value.as_reference(),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
            VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
        };
        let lhs = match &lhs {
            VariableValue::Thing(Thing::Attribute(attr)) => {
                attr.get_value(context.snapshot.as_ref(), context.thing_manager.as_ref(), storage_counters.clone())?
            }
            VariableValue::Value(value) => value.as_reference(),
            VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
            VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
        };
        if rhs.value_type().is_trivially_castable_to(lhs.value_type().category()) {
            Ok(Self::cmp_values_fn(&comparator)(&lhs, &rhs.cast(lhs.value_type().category()).unwrap()))
        } else if lhs.value_type().is_trivially_castable_to(rhs.value_type().category()) {
            Ok(Self::cmp_values_fn(&comparator)(&lhs.cast(rhs.value_type().category()).unwrap(), &rhs))
        } else {
            Ok(false)
        }
    }

    fn cmp_values_fn(comparator: &Comparator) -> fn(&Value<'_>, &Value<'_>) -> bool {
        match comparator {
            Comparator::Equal => |a, b| a == b,
            Comparator::NotEqual => |a, b| a != b,
            Comparator::Less => |a, b| a < b,
            Comparator::Greater => |a, b| a > b,
            Comparator::LessOrEqual => |a, b| a <= b,
            Comparator::GreaterOrEqual => |a, b| a >= b,
            Comparator::Like => |a, b| {
                // TODO: Avoid recompiling the regex every time.
                regex::Regex::new(b.unwrap_string_ref())
                    .expect("Invalid regex should have been caught at compile time")
                    .is_match(a.unwrap_string_ref())
            },
            Comparator::Contains => |a, b| {
                let a_unicase = UniCase::new(a.unwrap_string_ref()).to_folded_case();
                let b_unicase = UniCase::new(b.unwrap_string_ref()).to_folded_case();
                a_unicase.contains(b_unicase.as_str())
            },
        }
    }
}

fn make_const_extractor<T>(
    vertex: &CheckVertex<ExecutorVariable>,
    row: &MaybeOwnedRow<'_>,
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
) -> Box<dyn for<'a> Fn(&'a T) -> VariableValue<'a>> {
    let value = get_vertex_value(vertex, Some(row), &context.parameters);
    let owned_value = value.into_owned();
    Box::new(move |_| owned_value.clone())
}

fn get_vertex_value<'a, 'b>(
    vertex: &'a CheckVertex<ExecutorVariable>,
    row: Option<&'b MaybeOwnedRow<'b>>,
    parameters: &'b ParameterRegistry,
) -> VariableValue<'b> {
    match vertex {
        CheckVertex::Variable(var) => match var {
            ExecutorVariable::RowPosition(position) => {
                row.expect("CheckVertex::Variable requires a row to take from").get(*position).as_reference()
            }
            ExecutorVariable::Internal(_) => {
                unreachable!("Check variables without an extractor must have been recorded in the row.")
            }
        },
        CheckVertex::Type(type_) => VariableValue::Type(*type_),
        CheckVertex::Parameter(parameter_id) => {
            VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
        }
    }
}

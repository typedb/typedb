/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{Bound, HashMap},
    fmt,
    fmt::Formatter,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

use answer::{Thing, Type, variable_value::VariableValue};
use bytes::byte_array::ByteArray;
use compiler::{
    ExecutorVariable, VariablePosition,
    executable::match_::instructions::{CheckInstruction, CheckVertex},
};
use concept::{
    error::ConceptReadError,
    thing::{ThingAPI, object::ObjectAPI, thing_manager::ThingManager},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::{
    AsBytes,
    graph::thing::THING_VERTEX_MAX_LENGTH,
    value::{ValueEncodable, value::Value},
};
use error::unimplemented_feature;
use ir::{
    pattern::{
        IrID,
        constraint::{Comparator, IsaKind, SubKind},
    },
    pipeline::ParameterRegistry,
};
use itertools::Itertools;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use unicase::UniCase;

use crate::{pipeline::stage::ExecutionContext, row::MaybeOwnedRow};

#[derive(Debug)]
pub(crate) struct Checker<T: 'static> {
    extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    pub checks: Vec<CheckInstruction<ExecutorVariable>>,
    inline_check_factory: InlineCheckFactory<T>,
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
        let inline_check_factory = InlineCheckFactory::new(&checks, &extractors);
        Self { extractors, checks, inline_check_factory }
    }

    pub(crate) fn filter_fn_for_row<Snapshot: ReadableSnapshot + 'static>(
        &self,
        context: &ExecutionContext<Snapshot>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> + use<T, Snapshot> {
        self.inline_check_factory.filter_fn_for_row(context, row, storage_counters)
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
}

impl Checker<()> {
    pub(crate) fn filter(
        checks: &[CheckInstruction<ExecutorVariable>],
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        filter_impl(
            checks,
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            &context.parameters,
            row,
            storage_counters,
        )
    }
}

pub(crate) fn filter_impl<Source, Var: ExtractFrom<Source>>(
    checks: &[CheckInstruction<Var>],
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    for check in checks {
        let passes = match check {
            CheckInstruction::Iid { var, iid } => filter_iid(parameters, source, var, iid),
            CheckInstruction::TypeList { type_var, types } => filter_type_list(source, type_var, types),
            CheckInstruction::ThingTypeList { thing_var, types } => filter_thing_type_list(source, thing_var, types),
            CheckInstruction::Sub { sub_kind, subtype, supertype } => {
                filter_sub(snapshot, thing_manager, parameters, source, *sub_kind, subtype, supertype)?
            }
            CheckInstruction::Owns { owner, attribute } => {
                filter_owns(snapshot, thing_manager, parameters, source, owner, attribute)?
            }
            CheckInstruction::Relates { relation, role_type } => {
                filter_relates(snapshot, thing_manager, parameters, source, relation, role_type)?
            }
            CheckInstruction::Plays { player, role_type } => {
                filter_plays(snapshot, thing_manager, parameters, source, player, role_type)?
            }
            CheckInstruction::Isa { isa_kind, type_, thing } => {
                filter_isa(snapshot, thing_manager, parameters, source, *isa_kind, type_, thing)?
            }
            CheckInstruction::Has { owner, attribute } => {
                filter_has(snapshot, thing_manager, parameters, source, owner, attribute, storage_counters.clone())?
            }
            CheckInstruction::Links { relation, player, role } => filter_links(
                snapshot,
                thing_manager,
                parameters,
                source,
                relation,
                player,
                role,
                storage_counters.clone(),
            )?,
            CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                filter_indexed_relation(
                    snapshot,
                    thing_manager,
                    parameters,
                    source,
                    start_player,
                    end_player,
                    relation,
                    start_role,
                    end_role,
                    storage_counters.clone(),
                )?
            }
            CheckInstruction::Is { lhs, rhs } => filter_is(source, lhs, rhs),
            CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                filter_links_dedup(source, role1, player1, role2, player2)
            }
            CheckInstruction::Comparison { lhs, rhs, comparator } => filter_comparison(
                snapshot,
                thing_manager,
                parameters,
                source,
                lhs,
                rhs,
                *comparator,
                storage_counters.clone(),
            )?,
            CheckInstruction::NotNone { variables } => filter_not_none(source, variables),
            CheckInstruction::Unsatisfiable => false,
        };
        if !passes {
            return Ok(false);
        }
    }
    Ok(true)
}

fn filter_iid<Source, Var: ExtractFrom<Source>>(
    parameters: &ParameterRegistry,
    source: &Source,
    var: &Var,
    iid: &ir::pattern::ParameterID,
) -> bool {
    let extracted = var.extract(source);
    let iid = parameters.iid(iid).unwrap();
    check_iid(iid, extracted)
}

fn filter_type_list<Source, Var: ExtractFrom<Source>>(
    source: &Source,
    type_var: &Var,
    types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
) -> bool {
    let extracted = type_var.extract(source);
    let VariableValue::Type(t) = extracted else { return false };
    types.contains(&t)
}

fn filter_thing_type_list<Source, Var: ExtractFrom<Source>>(
    source: &Source,
    thing_var: &Var,
    types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
) -> bool {
    let extracted = thing_var.extract(source);
    let VariableValue::Thing(thing) = extracted else { return false };
    types.contains(&thing.type_())
}

fn filter_sub<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    sub_kind: SubKind,
    subtype: &CheckVertex<Var>,
    supertype: &CheckVertex<Var>,
) -> Result<bool, Box<ConceptReadError>> {
    let subtype = Var::extract_vertex(subtype, source, parameters);
    let supertype = Var::extract_vertex(supertype, source, parameters);
    check_sub(
        snapshot,
        thing_manager,
        sub_kind,
        unwrap_or_result_false!(subtype => Type),
        unwrap_or_result_false!(supertype => Type),
    )
}

fn filter_owns<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    owner: &CheckVertex<Var>,
    attribute: &CheckVertex<Var>,
) -> Result<bool, Box<ConceptReadError>> {
    let owner = Var::extract_vertex(owner, source, parameters);
    let attribute = Var::extract_vertex(attribute, source, parameters);
    let owner = unwrap_or_result_false!(owner => Type).as_object_type();
    let attribute = unwrap_or_result_false!(attribute => Type).as_attribute_type();
    owner.get_owns_attribute(snapshot, thing_manager.type_manager(), attribute).map(|owns| owns.is_some())
}

fn filter_relates<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    relation: &CheckVertex<Var>,
    role_type: &CheckVertex<Var>,
) -> Result<bool, Box<ConceptReadError>> {
    let relation = Var::extract_vertex(relation, source, parameters);
    let role_type = Var::extract_vertex(role_type, source, parameters);
    let relation_type = unwrap_or_result_false!(relation => Type).as_relation_type();
    let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
    relation_type.get_relates_role(snapshot, thing_manager.type_manager(), role_type).map(|r| r.is_some())
}

fn filter_plays<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    player: &CheckVertex<Var>,
    role_type: &CheckVertex<Var>,
) -> Result<bool, Box<ConceptReadError>> {
    let player = Var::extract_vertex(player, source, parameters);
    let role_type = Var::extract_vertex(role_type, source, parameters);
    let object_type = unwrap_or_result_false!(player => Type).as_object_type();
    let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
    object_type.get_plays_role(snapshot, thing_manager.type_manager(), role_type).map(|p| p.is_some())
}

fn filter_isa<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    isa_kind: IsaKind,
    type_: &CheckVertex<Var>,
    thing: &CheckVertex<Var>,
) -> Result<bool, Box<ConceptReadError>> {
    let thing = Var::extract_vertex(thing, source, parameters);
    let type_ = Var::extract_vertex(type_, source, parameters);
    let actual = unwrap_or_result_false!(thing => Thing).type_();
    let expected = unwrap_or_result_false!(type_ => Type);
    if isa_kind == IsaKind::Exact {
        Ok(actual == expected)
    } else {
        actual.is_transitive_subtype_of(expected, snapshot, thing_manager.type_manager())
    }
}

fn filter_has<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    owner: &CheckVertex<Var>,
    attribute: &CheckVertex<Var>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let owner = Var::extract_vertex(owner, source, parameters);
    let attribute = Var::extract_vertex(attribute, source, parameters);
    let owner = unwrap_or_result_false!(&owner => Thing).as_object();
    let attribute = unwrap_or_result_false!(&attribute => Thing).as_attribute();
    owner.has_attribute(snapshot, thing_manager, attribute, storage_counters)
}

fn filter_links<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    relation: &CheckVertex<Var>,
    player: &CheckVertex<Var>,
    role: &CheckVertex<Var>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let relation = Var::extract_vertex(relation, source, parameters);
    let player = Var::extract_vertex(player, source, parameters);
    let role = Var::extract_vertex(role, source, parameters);
    let relation = unwrap_or_result_false!(relation => Thing).as_relation();
    let player = unwrap_or_result_false!(player => Thing).as_object();
    let role = unwrap_or_result_false!(role => Type).as_role_type();
    relation.has_role_player(snapshot, thing_manager, player, role, storage_counters)
}

fn filter_indexed_relation<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    start_player: &CheckVertex<Var>,
    end_player: &CheckVertex<Var>,
    relation: &CheckVertex<Var>,
    start_role: &CheckVertex<Var>,
    end_role: &CheckVertex<Var>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let start_player = Var::extract_vertex(start_player, source, parameters);
    let end_player = Var::extract_vertex(end_player, source, parameters);
    let relation = Var::extract_vertex(relation, source, parameters);
    let start_role = Var::extract_vertex(start_role, source, parameters);
    let end_role = Var::extract_vertex(end_role, source, parameters);
    let start_player = unwrap_or_result_false!(start_player => Thing).as_object();
    let end_player = unwrap_or_result_false!(end_player => Thing).as_object();
    let relation = unwrap_or_result_false!(relation => Thing).as_relation();
    let start_role = unwrap_or_result_false!(start_role => Type).as_role_type();
    let end_role = unwrap_or_result_false!(end_role => Type).as_role_type();
    start_player.has_indexed_relation_player(
        snapshot,
        thing_manager,
        end_player,
        relation,
        start_role,
        end_role,
        storage_counters,
    )
}

fn filter_is<Source, Var: ExtractFrom<Source>>(row: &Source, lhs: &Var, rhs: &Var) -> bool {
    let lhs = Var::extract(lhs, row);
    let rhs = Var::extract(rhs, row);
    lhs == rhs
}

fn filter_links_dedup<Source, Var: ExtractFrom<Source>>(
    row: &Source,
    role1: &Var,
    player1: &Var,
    role2: &Var,
    player2: &Var,
) -> bool {
    let role1 = Var::extract(role1, row);
    let player1 = Var::extract(player1, row);
    let role2 = Var::extract(role2, row);
    let player2 = Var::extract(player2, row);
    !(role1 == role2 && player1 == player2)
}

fn filter_not_none<Source, Var: ExtractFrom<Source>>(row: &Source, variables: &[Var]) -> bool {
    variables.iter().all(|var| {
        let value = Var::extract(var, row);
        !value.is_none()
    })
}

fn filter_comparison<Source, Var: ExtractFrom<Source>>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
    source: &Source,
    lhs: &CheckVertex<Var>,
    rhs: &CheckVertex<Var>,
    comparator: Comparator,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let lhs = Var::extract_vertex(lhs, source, parameters);
    let rhs = Var::extract_vertex(rhs, source, parameters);
    let rhs = match &rhs {
        VariableValue::Thing(Thing::Attribute(attr)) => {
            attr.get_value(snapshot, thing_manager, storage_counters.clone())?
        }
        VariableValue::Value(value) => value.as_reference(),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
    };
    let lhs = match &lhs {
        VariableValue::Thing(Thing::Attribute(attr)) => {
            attr.get_value(snapshot, thing_manager, storage_counters.clone())?
        }
        VariableValue::Value(value) => value.as_reference(),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
    };
    if rhs.value_type().is_trivially_castable_to(lhs.value_type().category()) {
        Ok(cmp_values(&comparator)(&lhs, &rhs.cast(lhs.value_type().category()).unwrap()))
    } else if lhs.value_type().is_trivially_castable_to(rhs.value_type().category()) {
        Ok(cmp_values(&comparator)(&lhs.cast(rhs.value_type().category()).unwrap(), &rhs))
    } else {
        Ok(false)
    }
}

fn cmp_values(comparator: &Comparator) -> fn(&Value<'_>, &Value<'_>) -> bool {
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

fn get_vertex_value<'a, 'b>(
    vertex: &'a CheckVertex<ExecutorVariable>,
    row: Option<&'b MaybeOwnedRow<'b>>,
    parameters: &'b ParameterRegistry,
) -> VariableValue<'b> {
    match vertex {
        CheckVertex::Variable(var) => get_variable_value(row, &var),
        CheckVertex::Type(type_) => VariableValue::Type(*type_),
        CheckVertex::Parameter(parameter_id) => {
            VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
        }
    }
}

fn get_variable_value<'a>(row: Option<&'a MaybeOwnedRow<'a>>, variable: &ExecutorVariable) -> VariableValue<'a> {
    match variable {
        ExecutorVariable::RowPosition(position) => {
            row.expect("CheckVertex::Variable requires a row to take from").get(*position).as_reference()
        }
        ExecutorVariable::Internal(_) => {
            unreachable!("Check variables without an extractor must have been recorded in the row.")
        }
    }
}

trait ExtractFrom<Source>: Sized {
    fn extract_vertex<'a>(
        vertex: &'a CheckVertex<Self>,
        from: &'a Source,
        parameters: &'a ParameterRegistry,
    ) -> VariableValue<'a> {
        match vertex {
            CheckVertex::Variable(var) => var.extract(from),
            CheckVertex::Type(type_) => VariableValue::Type(*type_),
            CheckVertex::Parameter(parameter_id) => {
                VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
            }
        }
    }

    fn extract<'a>(&'a self, from: &'a Source) -> VariableValue<'a>;
}

impl<'r> ExtractFrom<MaybeOwnedRow<'r>> for ExecutorVariable {
    fn extract<'a>(&'a self, row: &'a MaybeOwnedRow<'r>) -> VariableValue<'a> {
        match self {
            ExecutorVariable::RowPosition(position) => row.get(*position).as_reference(),
            ExecutorVariable::Internal(_) => {
                unreachable!("Check variables without an extractor must have been recorded in the row.")
            }
        }
    }
}

type SubRow = Vec<VariableValue<'static>>;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct SubRowIndex(usize);

enum FilterFnVariable<T> {
    ExtractFromSubRow(SubRowIndex),
    ExtractFromTuple(fn(&T) -> VariableValue<'_>),
}

impl<T> Clone for FilterFnVariable<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for FilterFnVariable<T> {}
impl<T> fmt::Debug for FilterFnVariable<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExtractFromSubRow(i) => write!(f, "SubRowIndex({:?})", i),
            Self::ExtractFromTuple(_) => f.write_str("ExtractFromTuple(...)"),
        }
    }
}

impl<T> Hash for FilterFnVariable<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::ExtractFromSubRow(i) => {
                0u8.hash(state);
                i.hash(state);
            }
            Self::ExtractFromTuple(f) => {
                1u8.hash(state);
                (*f as usize).hash(state);
            }
        }
    }
}
impl<T> PartialEq for FilterFnVariable<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ExtractFromSubRow(a), Self::ExtractFromSubRow(b)) => a == b,
            (Self::ExtractFromTuple(a), Self::ExtractFromTuple(b)) => *a as usize == *b as usize,
            _ => false,
        }
    }
}
impl<T> Eq for FilterFnVariable<T> {}
impl<T> PartialOrd for FilterFnVariable<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for FilterFnVariable<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::ExtractFromSubRow(a), Self::ExtractFromSubRow(b)) => a.cmp(b),
            (Self::ExtractFromTuple(a), Self::ExtractFromTuple(b)) => (*a as usize).cmp(&(*b as usize)),
            (Self::ExtractFromSubRow(_), Self::ExtractFromTuple(_)) => std::cmp::Ordering::Less,
            (Self::ExtractFromTuple(_), Self::ExtractFromSubRow(_)) => std::cmp::Ordering::Greater,
        }
    }
}

impl<T: 'static> IrID for FilterFnVariable<T> {}

impl<T> fmt::Display for FilterFnVariable<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FilterFnVariable::ExtractFromSubRow(index) => write!(f, "SubRowIndex[{}]", index.0),
            FilterFnVariable::ExtractFromTuple(_) => f.write_str("<tuple-extractor>"),
        }
    }
}

struct TupleAndSubRow<'a, T> {
    tuple: &'a T,
    subrow: &'a SubRow,
}

impl<T> ExtractFrom<TupleAndSubRow<'_, T>> for FilterFnVariable<T> {
    fn extract<'a>(&'a self, from: &'a TupleAndSubRow<'_, T>) -> VariableValue<'a> {
        match self {
            Self::ExtractFromTuple(f) => f(from.tuple),
            Self::ExtractFromSubRow(index) => from.subrow[index.0].as_reference(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct InlineCheckFactory<T> {
    checks: Arc<Vec<CheckInstruction<FilterFnVariable<T>>>>,
    subrow_schema: Vec<VariablePosition>,
}

impl<T: 'static> InlineCheckFactory<T> {
    pub(crate) fn new(
        outline_checks: &[CheckInstruction<ExecutorVariable>],
        extractors: &HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        let mut subrow_schema = Vec::new();
        let mut mapping = HashMap::new();
        for check in outline_checks {
            for variable in check.ids() {
                if mapping.contains_key(&variable) {
                    continue;
                }
                let mapped_variable = if let Some(extractor) = extractors.get(&variable) {
                    FilterFnVariable::ExtractFromTuple(*extractor)
                } else {
                    let ExecutorVariable::RowPosition(position) = variable else {
                        unreachable!("Check variables without an extractor must be in the row.")
                    };
                    let subrow_index = subrow_schema.len();
                    subrow_schema.push(position);
                    FilterFnVariable::ExtractFromSubRow(SubRowIndex(subrow_index))
                };
                mapping.insert(variable, mapped_variable);
            }
        }
        let mut checks = Vec::with_capacity(outline_checks.len());
        for check in outline_checks {
            checks.push(check.clone().map(&mapping));
        }
        Self { checks: Arc::new(checks), subrow_schema }
    }

    pub(crate) fn filter_fn_for_row<Snapshot: ReadableSnapshot + 'static>(
        &self,
        context: &ExecutionContext<Snapshot>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> + use<T, Snapshot> {
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let parameters = context.parameters.clone();
        self.make_filter_fn_for_row(row).into_filter_fn(snapshot, thing_manager, parameters, storage_counters)
    }

    fn make_filter_fn_for_row(&self, row: &MaybeOwnedRow<'_>) -> FilterFnWithSubRow<T> {
        let mut subrow = Vec::with_capacity(self.subrow_schema.len());
        for &pos in &self.subrow_schema {
            subrow.push(row.get(pos).to_owned());
        }
        let checks = self.checks.clone();
        FilterFnWithSubRow { checks, subrow }
    }
}

pub(crate) struct FilterFnWithSubRow<T> {
    checks: Arc<Vec<CheckInstruction<FilterFnVariable<T>>>>,
    subrow: SubRow,
}

impl<T> FilterFnWithSubRow<T> {
    fn into_filter_fn(
        self,
        snapshot: Arc<impl ReadableSnapshot + 'static>,
        thing_manager: Arc<ThingManager>,
        parameters: Arc<ParameterRegistry>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> {
        move |res| {
            // TODO: Copied from the older one. Doesn't this swallow errors?
            let Ok(tuple) = res else { return Ok(true) };
            let value = TupleAndSubRow { tuple, subrow: &self.subrow };
            filter_impl(&self.checks, &*snapshot, &thing_manager, &parameters, &value, storage_counters.clone())
        }
    }
}

impl<T: 'static> FilterFnWithSubRow<T> {
    pub(crate) fn check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        item: &Result<T, Box<ConceptReadError>>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        // TODO: WHY DO THESE ACCEPT &Result<T, _> instead of just &T?
        let tuple = match item {
            Ok(tuple) => tuple,
            Err(err) => return Err(err.clone()),
        };
        let source = TupleAndSubRow { tuple, subrow: &self.subrow };
        filter_impl(
            &self.checks,
            &*context.snapshot,
            &context.thing_manager,
            &context.parameters,
            &source,
            storage_counters,
        )
    }
}

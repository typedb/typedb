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
    value::{
        ValueEncodable,
        value::{Value, ValueRestriction},
    },
};
use error::unimplemented_feature;
use ir::{
    pattern::{
        IrID,
        constraint::{Comparator, IsaKind, SubKind},
    },
    pipeline::ParameterRegistry,
};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use unicase::UniCase;

use crate::{pipeline::stage::ExecutionContext, row::MaybeOwnedRow};

#[derive(Debug)]
pub(crate) struct Checker<T: 'static> {
    checks: Vec<CheckInstruction<ExecutorVariable>>,
    inline_check_factory: InlineCheckFactory<T>,
}

macro_rules! unwrap_or_result_false {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return Ok(false) };
        x
    }};
}

impl<T> Checker<T> {
    pub(crate) fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        let inline_check_factory = InlineCheckFactory::new(&checks, &extractors);
        Self { checks, inline_check_factory }
    }

    pub(crate) fn filter_fn_for_row<Snapshot: ReadableSnapshot + 'static>(
        &self,
        context: &ExecutionContext<Snapshot>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> + use<T, Snapshot> {
        self.inline_check_factory.filter_fn_for_row(context, row, storage_counters)
    }

    pub(crate) fn value_restriction_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: Option<MaybeOwnedRow<'_>>,
        target_variable: ExecutorVariable,
        storage_counters: StorageCounters,
    ) -> Result<ValueRestriction<'static>, Box<ConceptReadError>> {
        let mut restriction = ValueRestriction::None;
        for check in &self.checks {
            // Normalise each check to `target <comparator> other`
            let (other, comparator) = match check {
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    if lhs.as_variable() == Some(target_variable) {
                        (rhs.clone(), *comparator)
                    } else {
                        debug_assert!(
                            rhs.as_variable().expect("RHS of comparison must be a variable") == target_variable
                        );
                        let Some(comparator) = comparator.reverse() else { continue };
                        (lhs.clone(), comparator)
                    }
                }
                CheckInstruction::Is { lhs, rhs } => {
                    let other = if *lhs == target_variable { *rhs } else { *lhs };
                    (CheckVertex::Variable(other), Comparator::Equal)
                }
                _ => continue,
            };
            let other_variable_value = get_vertex_value(&other, row.as_ref(), &context.parameters);
            let Some(other_value) = Self::read_value(
                context.snapshot.as_ref(),
                &context.thing_manager,
                &other_variable_value,
                storage_counters.clone(),
            )?
            else {
                continue;
            };
            if let Some(comparison_restriction) = Self::comparison_restriction(comparator, other_value) {
                restriction = restriction & comparison_restriction;
                if matches!(restriction, ValueRestriction::Unsatisfiable) {
                    break;
                }
            }
        }
        Ok(restriction)
    }

    fn comparison_restriction(comparator: Comparator, value: Value<'static>) -> Option<ValueRestriction<'static>> {
        match comparator {
            Comparator::Equal => Some(ValueRestriction::Equality(value)),
            Comparator::Less => Some(ValueRestriction::new_range(Bound::Unbounded, Bound::Excluded(value))),
            Comparator::LessOrEqual => Some(ValueRestriction::new_range(Bound::Unbounded, Bound::Included(value))),
            Comparator::Greater => Some(ValueRestriction::new_range(Bound::Excluded(value), Bound::Unbounded)),
            Comparator::GreaterOrEqual => Some(ValueRestriction::new_range(Bound::Included(value), Bound::Unbounded)),
            Comparator::NotEqual | Comparator::Like | Comparator::Contains => None,
        }
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
        all_checks_pass(
            checks,
            context.snapshot.as_ref(),
            context.thing_manager.as_ref(),
            &context.parameters,
            row,
            storage_counters,
        )
    }
}

fn all_checks_pass<Source, Var: ExtractFrom<Source>>(
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
    types: &Arc<std::collections::BTreeSet<Type>>,
) -> bool {
    let extracted = type_var.extract(source);
    let VariableValue::Type(t) = extracted else { return false };
    types.contains(&t)
}

fn filter_thing_type_list<Source, Var: ExtractFrom<Source>>(
    source: &Source,
    thing_var: &Var,
    types: &Arc<std::collections::BTreeSet<Type>>,
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

fn filter_is<Source, Var: ExtractFrom<Source>>(source: &Source, lhs: &Var, rhs: &Var) -> bool {
    let lhs = lhs.extract(source);
    let rhs = rhs.extract(source);
    lhs == rhs
}

fn filter_links_dedup<Source, Var: ExtractFrom<Source>>(
    source: &Source,
    role1: &Var,
    player1: &Var,
    role2: &Var,
    player2: &Var,
) -> bool {
    let role1 = role1.extract(source);
    let player1 = player1.extract(source);
    let role2 = role2.extract(source);
    let player2 = player2.extract(source);
    !(role1 == role2 && player1 == player2)
}

fn filter_not_none<Source, Var: ExtractFrom<Source>>(source: &Source, variables: &[Var]) -> bool {
    variables.iter().all(|var| {
        let value = var.extract(source);
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
    if matches!(lhs, Value::Vector(_)) || matches!(rhs, Value::Vector(_)) {
        return Ok(matches!((&lhs, &rhs), (Value::Vector(_), Value::Vector(_))) && cmp_values(&comparator)(&lhs, &rhs));
    }
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

fn get_vertex_value<'a>(
    vertex: &'a CheckVertex<ExecutorVariable>,
    row: Option<&'a MaybeOwnedRow<'a>>,
    parameters: &'a ParameterRegistry,
) -> VariableValue<'a> {
    match vertex {
        CheckVertex::Variable(var) => {
            let row = row.expect("CheckVertex::Variable requires a row to take from");
            var.extract(row)
        }
        CheckVertex::Type(type_) => VariableValue::Type(*type_),
        CheckVertex::Parameter(parameter_id) => {
            VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
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

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct CheckInputRowIndex(usize);

enum InlineCheckVariable<T> {
    ExtractFromRow(CheckInputRowIndex),
    ExtractFromTuple(fn(&T) -> VariableValue<'_>),
}

impl<T> fmt::Debug for InlineCheckVariable<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExtractFromRow(i) => write!(f, "CheckInputRowIndex({:?})", i.0),
            Self::ExtractFromTuple(_) => f.write_str("ExtractFromTuple(...)"),
        }
    }
}

struct TupleAndInlineCheckRow<'a, T> {
    tuple: &'a T,
    check_input_row: &'a [VariableValue<'static>],
}

impl<T> ExtractFrom<TupleAndInlineCheckRow<'_, T>> for InlineCheckVariable<T> {
    fn extract<'a>(&'a self, from: &'a TupleAndInlineCheckRow<'_, T>) -> VariableValue<'a> {
        match self {
            Self::ExtractFromTuple(f) => f(from.tuple),
            Self::ExtractFromRow(index) => from.check_input_row[index.0].as_reference(),
        }
    }
}

#[derive(Debug)]
struct InlineCheckFactory<T> {
    checks: Arc<Vec<CheckInstruction<InlineCheckVariable<T>>>>,
    check_input_row_sources: Vec<VariablePosition>,
}

impl<T: 'static> InlineCheckFactory<T> {
    fn new(
        outline_checks: &[CheckInstruction<ExecutorVariable>],
        extractors: &HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        let mut check_input_row_sources = Vec::new();
        let mut check_mapper = CheckMapper { extractors, check_input_row_sources: &mut check_input_row_sources };
        let mut checks = Vec::with_capacity(outline_checks.len());
        for check in outline_checks {
            checks.push(check_mapper.map_check(check.clone()));
        }
        Self { checks: Arc::new(checks), check_input_row_sources }
    }

    fn filter_fn_for_row<Snapshot: ReadableSnapshot + 'static>(
        &self,
        context: &ExecutionContext<Snapshot>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> + use<T, Snapshot> {
        let snapshot = context.snapshot.clone();
        let thing_manager = context.thing_manager.clone();
        let parameters = context.parameters.clone();
        self.make_inline_check_for_row(row).into_filter_fn(snapshot, thing_manager, parameters, storage_counters)
    }

    fn make_inline_check_for_row(&self, row: &MaybeOwnedRow<'_>) -> RowBoundInlineCheck<T> {
        let mut check_input_row = Vec::with_capacity(self.check_input_row_sources.len());
        for &pos in &self.check_input_row_sources {
            check_input_row.push(row.get(pos).to_owned());
        }
        let checks = self.checks.clone();
        RowBoundInlineCheck { checks, check_input_row }
    }
}

struct RowBoundInlineCheck<T> {
    checks: Arc<Vec<CheckInstruction<InlineCheckVariable<T>>>>,
    check_input_row: Vec<VariableValue<'static>>,
}

impl<T> RowBoundInlineCheck<T> {
    fn into_filter_fn(
        self,
        snapshot: Arc<impl ReadableSnapshot + 'static>,
        thing_manager: Arc<ThingManager>,
        parameters: Arc<ParameterRegistry>,
        storage_counters: StorageCounters,
    ) -> impl Fn(&Result<T, Box<ConceptReadError>>) -> Result<bool, Box<ConceptReadError>> {
        move |res| {
            // returning Ok(true) on error just means the error is passed on to the next layer.
            let Ok(tuple) = res else { return Ok(true) };
            let value = TupleAndInlineCheckRow { tuple, check_input_row: &self.check_input_row };
            all_checks_pass(&self.checks, &*snapshot, &thing_manager, &parameters, &value, storage_counters.clone())
        }
    }
}

struct CheckMapper<'a, T> {
    extractors: &'a HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    check_input_row_sources: &'a mut Vec<VariablePosition>,
}

impl<'a, T> CheckMapper<'a, T> {
    fn map_check(
        &mut self,
        check_instruction: CheckInstruction<ExecutorVariable>,
    ) -> CheckInstruction<InlineCheckVariable<T>> {
        match check_instruction {
            CheckInstruction::TypeList { type_var, types } => {
                CheckInstruction::TypeList { type_var: self.map_var(type_var), types }
            }
            CheckInstruction::ThingTypeList { thing_var, types } => {
                CheckInstruction::ThingTypeList { thing_var: self.map_var(thing_var), types }
            }
            CheckInstruction::Iid { var, iid } => CheckInstruction::Iid { var: self.map_var(var), iid },
            CheckInstruction::Sub { sub_kind: kind, subtype, supertype } => {
                CheckInstruction::Sub { sub_kind: kind, subtype: self.map(subtype), supertype: self.map(supertype) }
            }
            CheckInstruction::Owns { owner, attribute } => {
                CheckInstruction::Owns { owner: self.map(owner), attribute: self.map(attribute) }
            }
            CheckInstruction::Relates { relation, role_type } => {
                CheckInstruction::Relates { relation: self.map(relation), role_type: self.map(role_type) }
            }
            CheckInstruction::Plays { player, role_type } => {
                CheckInstruction::Plays { player: self.map(player), role_type: self.map(role_type) }
            }
            CheckInstruction::Isa { isa_kind: kind, type_, thing } => {
                CheckInstruction::Isa { isa_kind: kind, type_: self.map(type_), thing: self.map(thing) }
            }
            CheckInstruction::Has { owner, attribute } => {
                CheckInstruction::Has { owner: self.map(owner), attribute: self.map(attribute) }
            }
            CheckInstruction::Links { relation, player, role } => {
                CheckInstruction::Links { relation: self.map(relation), player: self.map(player), role: self.map(role) }
            }
            CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                CheckInstruction::IndexedRelation {
                    relation: self.map(relation),
                    start_player: self.map(start_player),
                    end_player: self.map(end_player),
                    start_role: self.map(start_role),
                    end_role: self.map(end_role),
                }
            }
            CheckInstruction::Is { lhs, rhs } => {
                CheckInstruction::Is { lhs: self.map_var(lhs), rhs: self.map_var(rhs) }
            }
            CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                CheckInstruction::LinksDeduplication {
                    role1: self.map_var(role1),
                    player1: self.map_var(player1),
                    role2: self.map_var(role2),
                    player2: self.map_var(player2),
                }
            }
            CheckInstruction::NotNone { variables } => {
                CheckInstruction::NotNone { variables: variables.into_iter().map(|v| self.map_var(v)).collect() }
            }
            CheckInstruction::Comparison { lhs, rhs, comparator } => {
                CheckInstruction::Comparison { lhs: self.map(lhs), rhs: self.map(rhs), comparator }
            }
            CheckInstruction::Unsatisfiable => CheckInstruction::Unsatisfiable,
        }
    }

    fn map(&mut self, vertex: CheckVertex<ExecutorVariable>) -> CheckVertex<InlineCheckVariable<T>> {
        match vertex {
            CheckVertex::Variable(var) => CheckVertex::Variable(self.map_var(var)),
            CheckVertex::Type(label) => CheckVertex::Type(label),
            CheckVertex::Parameter(param) => CheckVertex::Parameter(param),
        }
    }

    fn map_var(&mut self, variable: ExecutorVariable) -> InlineCheckVariable<T> {
        if let Some(extractor) = self.extractors.get(&variable) {
            InlineCheckVariable::ExtractFromTuple(*extractor)
        } else {
            let ExecutorVariable::RowPosition(position) = variable else {
                unreachable!("Check variables without an extractor must be in the row.")
            };
            let check_input_row_index = self.check_input_row_sources.len();
            self.check_input_row_sources.push(position);
            InlineCheckVariable::ExtractFromRow(CheckInputRowIndex(check_input_row_index))
        }
    }
}

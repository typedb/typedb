/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, nested_pattern::NestedPattern},
    pipeline::function_signature::{FunctionID, FunctionIDAPI},
};

use crate::{
    annotation::{function::AnnotatedFunction, pipeline::AnnotatedStage},
    executable::{
        function::{FunctionTablingType, StronglyConnectedComponentID},
        ExecutableCompilationError,
    },
};

pub(super) fn determine_compilation_order_and_tabling_types<FIDType: FunctionIDAPI>(
    to_compile: &HashMap<FIDType, AnnotatedFunction>,
) -> Result<(Vec<FIDType>, HashMap<FIDType, FunctionTablingType>), ExecutableCompilationError> {
    let mut forward_dependencies = HashMap::new();
    to_compile.keys().try_for_each(|fid| collect_dependencies(to_compile, &mut forward_dependencies, fid))?;
    debug_assert!(to_compile.len() == forward_dependencies.len() && to_compile.keys().all(|k| forward_dependencies.contains_key(k)));

    // Find the post_order
    let (post_order, cycle_breakers) = determine_post_order_and_cycle_breakers(&forward_dependencies);
    debug_assert!(to_compile.keys().all(|k| post_order.contains(k)) && post_order.len() == to_compile.len());

    // Reverse forward dependencies
    let mut reverse_dependencies = to_compile.keys().map(|k| (k.clone(), HashSet::new())).collect::<HashMap<_, _>>();
    forward_dependencies.iter().for_each(|(k, v_set)| {
        v_set.iter().for_each(|v| {
            reverse_dependencies.get_mut(v).unwrap().insert(k.clone());
        });
    });

    // Use the post-order & reversed graph to find SCCs using Kosaraju
    let mut scc_mapping: HashMap<FIDType, StronglyConnectedComponentID> = HashMap::new();
    post_order.iter().rev().for_each(|root| {
        assign_scc(&reverse_dependencies, &mut scc_mapping, root, root);
    });

    // Convert SCCID to TablingTypes
    debug_assert!(scc_mapping.len() == to_compile.len() && to_compile.keys().all(|k| scc_mapping.contains_key(k)));
    let tabling_types = scc_mapping.into_iter().map(|(fid, scc)| match cycle_breakers.contains(&fid) {
        true => (fid.clone(), FunctionTablingType::Tabled(scc)),
         false => (fid, FunctionTablingType::Untabled)
    }).collect::<HashMap<_, _>>();

    Ok((post_order, tabling_types))
}

fn collect_dependencies<FIDType: FunctionIDAPI>(
    to_compile: &HashMap<FIDType, AnnotatedFunction>,
    forward_dependencies: &mut HashMap<FIDType, HashSet<FIDType>>,
    fid: &FIDType,
) -> Result<(), ExecutableCompilationError> {
    if forward_dependencies.contains_key(&fid) {
        return Ok(());
    }

    let function = to_compile.get(&fid).unwrap();
    let all_called_ids = all_calls_in_pipeline(function.stages.as_slice())
        .iter()
        .filter_map(|id| FIDType::try_from(id.clone()).ok())
        .filter(|id| to_compile.contains_key(id))
        .collect::<HashSet<_>>();
    forward_dependencies.insert(fid.clone(), all_called_ids.clone());

    all_called_ids.iter().try_for_each(|dep| collect_dependencies(to_compile, forward_dependencies, dep))?;
    Ok(())
}

fn determine_post_order_and_cycle_breakers<FIDType: FunctionIDAPI>(
    forward_dependencies: &HashMap<FIDType, HashSet<FIDType>>,
) -> (Vec<FIDType>, HashSet<FIDType>) {
    let mut post_order = Vec::new();
    let mut cycle_breakers = HashSet::new();
    let mut open_set = HashSet::new();
    let mut closed_set = HashSet::new();
    forward_dependencies.keys().for_each(|fid| {
        determine_post_order_and_cycle_breakers_impl(
            &forward_dependencies,
            &mut post_order,
            &mut cycle_breakers,
            &mut open_set,
            &mut closed_set,
            fid,
        );
    });
    (post_order, cycle_breakers)
}

fn determine_post_order_and_cycle_breakers_impl<FIDType: FunctionIDAPI>(
    forward_dependencies: &HashMap<FIDType, HashSet<FIDType>>,
    post_order: &mut Vec<FIDType>,
    cycle_breakers: &mut HashSet<FIDType>,
    open: &mut HashSet<FIDType>,
    closed: &mut HashSet<FIDType>,
    fid: &FIDType,
) {
    if closed.contains(fid) {
        return;
    }
    if open.contains(fid) {
        cycle_breakers.insert(fid.clone());
        return;
    }

    open.insert(fid.clone());
    forward_dependencies.get(fid).unwrap().iter().for_each(|dep| {
        determine_post_order_and_cycle_breakers_impl(forward_dependencies, post_order, cycle_breakers, open, closed, dep);
    });
    open.remove(fid);
    closed.insert(fid.clone());

    post_order.push(fid.clone());
}

fn assign_scc<FIDType: FunctionIDAPI>(
    reversed_dependencies: &HashMap<FIDType, HashSet<FIDType>>,
    scc_mapping: &mut HashMap<FIDType, StronglyConnectedComponentID>,
    root: &FIDType,
    fid: &FIDType,
) {
    if scc_mapping.contains_key(fid) {
        return;
    }
    scc_mapping.insert(fid.clone(), StronglyConnectedComponentID(root.clone().into()));
    reversed_dependencies.get(fid).unwrap().iter().for_each(|rdep| {
        assign_scc(reversed_dependencies, scc_mapping, root, rdep);
    })
}

pub(super) fn all_calls_in_pipeline(stages: &[AnnotatedStage]) -> HashSet<FunctionID> {
    let match_stage_conjunctions = stages.iter().filter_map(|stage| match stage {
        AnnotatedStage::Match { block, .. } => Some(block.conjunction()),
        _ => None,
    });
    let mut call_accumulator = HashSet::new();
    for conjunction in match_stage_conjunctions {
        all_calls_in_conjunction(conjunction, &mut call_accumulator);
    }
    call_accumulator
}

fn all_calls_in_conjunction(conjunction: &Conjunction, call_accumulator: &mut HashSet<FunctionID>) {
    for constraint in conjunction.constraints() {
        if let Constraint::FunctionCallBinding(binding) = constraint {
            call_accumulator.insert(binding.function_call().function_id());
        }
    }
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(disjunction) => {
                for inner in disjunction.conjunctions() {
                    all_calls_in_conjunction(inner, call_accumulator);
                }
            }
            NestedPattern::Optional(optional) => {
                all_calls_in_conjunction(optional.conjunction(), call_accumulator);
            }
            NestedPattern::Negation(negation) => {
                all_calls_in_conjunction(negation.conjunction(), call_accumulator);
            }
        }
    }
}

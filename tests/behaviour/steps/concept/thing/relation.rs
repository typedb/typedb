/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::slice;

use concept::{
    thing::object::{Object, ObjectAPI},
    type_::TypeAPI,
};
use cucumber::gherkin::Step;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;

use crate::{
    concept::type_::BehaviourConceptTestExecutionError,
    generic_step, params,
    params::{check_boolean, IsEmptyOrNot},
    transaction_context::{with_read_tx, with_write_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = r"relation {var} add player for role\({type_label}\): {var}{may_error}")]
async fn relation_add_player_for_role(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    player_var: params::Var,
    may_error: params::MayError,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let player = context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object.clone();
    with_write_tx!(context, |tx| {
        if let Some(relates) = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
        {
            let role_type = relates.role();
            let res = relation.add_player(&mut tx.snapshot, &tx.thing_manager, role_type, player);
            may_error.check_concept_write_without_read_errors(&res);
            return;
        }
    });
    may_error.check::<(), BehaviourConceptTestExecutionError>(&Err(
        BehaviourConceptTestExecutionError::CannotFindRoleToAddPlayerTo,
    ));
}

#[apply(generic_step)]
#[step(expr = r"relation {var} set players for role\({type_label}[]\): {vars}{may_error}")]
async fn relation_set_players_for_role(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    player_vars: params::Vars,
    may_error: params::MayError,
) {
    let relation = context.objects[&relation_var.name].as_ref().unwrap().object.clone().unwrap_relation();
    let players =
        player_vars.names.into_iter().map(|name| context.objects[&name].as_ref().unwrap().object.clone()).collect_vec();
    let res = with_write_tx!(context, |tx| {
        let role_type = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        relation.set_players_ordered(&mut tx.snapshot, &tx.thing_manager, role_type, players)
    });
    may_error.check_concept_write_without_read_errors(&res);
}

#[apply(generic_step)]
#[step(expr = r"relation {var} remove player for role\({type_label}\): {var}")]
async fn relation_remove_player_for_role(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    player_var: params::Var,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let player = context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object.clone();
    with_write_tx!(context, |tx| {
        let role_type = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        relation.remove_player_single(&mut tx.snapshot, &tx.thing_manager, role_type, player).unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = r"relation {var} remove {int} players for role\({type_label}[]\): {var}")]
async fn relation_remove_count_players_for_role(
    context: &mut Context,
    relation_var: params::Var,
    count: u64,
    role_label: params::Label,
    player_var: params::Var,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let player = context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object.clone();
    with_write_tx!(context, |tx| {
        let role_type = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        relation.remove_player_many(&mut tx.snapshot, &tx.thing_manager, role_type, player, count).unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = r"{var} = relation {var} get players for role\({type_label}[]\)")]
async fn relation_get_players_ordered(
    context: &mut Context,
    players_var: params::Var,
    relation_var: params::Var,
    role_label: params::Label,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let players = with_read_tx!(context, |tx| {
        let relates = relation.type_().get_relates_role_name(
            &tx.snapshot,
            &tx.type_manager,
            role_label.into_typedb().name().as_str(),
        );
        let role_type = relates.unwrap().unwrap().role();
        let players = relation.get_players_ordered(&tx.snapshot, &tx.thing_manager, role_type).unwrap();
        players.into_iter().map(Object::into_owned).collect()
    });
    context.object_lists.insert(players_var.name, players);
}

#[apply(generic_step)]
#[step(expr = r"relation {var} get players for role\({type_label}[]\) is {vars}: {boolean}")]
async fn relation_get_players_ordered_is(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    player_vars: params::Vars,
    is: params::Boolean,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let actuals = with_read_tx!(context, |tx| {
        let relates = relation.type_().get_relates_role_name(
            &tx.snapshot,
            &tx.type_manager,
            role_label.into_typedb().name().as_str(),
        );
        let role_type = relates.unwrap().unwrap().role();
        let players = relation.get_players_ordered(&tx.snapshot, &tx.thing_manager, role_type).unwrap();
        players.into_iter().map(Object::into_owned).collect_vec()
    });
    let players =
        player_vars.names.into_iter().map(|name| context.objects[&name].as_ref().unwrap().object.clone()).collect_vec();
    check_boolean!(is, actuals == players)
}

#[apply(generic_step)]
#[step(expr = r"roleplayer {var}[{int}] is {var}")]
async fn roleplayer_list_at_index_is(
    context: &mut Context,
    list_var: params::Var,
    index: usize,
    roleplayer_var: params::Var,
) {
    let list_item = &context.object_lists[&list_var.name][index];
    let roleplayer = &context.objects[&roleplayer_var.name].as_ref().unwrap().object;
    assert_eq!(list_item, roleplayer);
}

#[apply(generic_step)]
#[step(expr = r"relation {var} get players {contains_or_doesnt}: {var}")]
async fn relation_get_players_contains(
    context: &mut Context,
    relation_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    player_var: params::Var,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let players = with_read_tx!(context, |tx| {
        relation
            .get_players(&tx.snapshot, &tx.thing_manager)
            .map_static(|result| (result.unwrap().0.player().into_owned()))
            .collect::<Vec<_>>()
    });
    let player = &context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object;
    contains_or_doesnt.check(slice::from_ref(player), &players);
}

#[apply(generic_step)]
#[step(expr = r"relation {var} get players {contains_or_doesnt}:")]
async fn relation_get_players_contains_table(
    context: &mut Context,
    step: &Step,
    relation_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let players = with_read_tx!(context, |tx| {
        let mut iter = relation.get_players(&tx.snapshot, &tx.thing_manager);

        let mut vec = Vec::new();
        while let Some(res) = iter.next() {
            let (rp, _count) = res.unwrap();
            vec.push((
                rp.role_type().get_label(&tx.snapshot, &tx.type_manager).unwrap().name().as_str().to_owned(),
                rp.player().into_owned(),
            ));
        }
        vec
    });
    let expected = step
        .table()
        .unwrap()
        .rows
        .iter()
        .map(|row| {
            let [role_label, var_name] = &row[..] else {
                panic!("Expected exactly two columns: role type and variable name, received: {row:?}")
            };
            (role_label.to_owned(), context.objects.get(var_name.as_str()).unwrap().as_ref().unwrap().object.clone())
        })
        .collect_vec();
    contains_or_doesnt.check(&expected, &players);
}

#[apply(generic_step)]
#[step(expr = r"relation {var} get players for role\({type_label}\) {is_empty_or_not}")]
async fn relation_get_players_for_role_empty(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    is_empty_or_not: IsEmptyOrNot,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let actuals = with_read_tx!(context, |tx| {
        let role_type = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        relation
            .get_players_role_type(&tx.snapshot, &tx.thing_manager, role_type)
            .map_static(|res| res.unwrap().into_owned())
            .collect::<Vec<_>>()
    });

    is_empty_or_not.check(actuals.is_empty());
}

#[apply(generic_step)]
#[step(expr = r"relation {var} get players for role\({type_label}\) {contains_or_doesnt}: {var}")]
async fn relation_get_players_for_role_contains(
    context: &mut Context,
    relation_var: params::Var,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    player_var: params::Var,
) {
    let relation = context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object.clone().unwrap_relation();
    let players = with_read_tx!(context, |tx| {
        let role_type = relation
            .type_()
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        relation
            .get_players_role_type(&tx.snapshot, &tx.thing_manager, role_type)
            .map_static(|res| res.unwrap().into_owned())
            .collect::<Vec<_>>()
    });
    let player = &context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object;
    contains_or_doesnt.check(slice::from_ref(player), &players);
}

#[apply(generic_step)]
#[step(expr = r"{object_root_label} {var} get relations {contains_or_doesnt}: {var}")]
async fn object_get_relations_contain(
    context: &mut Context,
    object_root: params::ObjectRootLabel,
    player_var: params::Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
    relation_var: params::Var,
) {
    let player = &context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object;
    object_root.assert(&player.type_());
    let relations = with_read_tx!(context, |tx| {
        player
            .get_relations(&tx.snapshot, &tx.thing_manager)
            .map_static(|rel| rel.unwrap().into_owned())
            .collect::<Vec<_>>()
    });
    let Object::Relation(relation) = &context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object else {
        panic!()
    };
    contains_or_doesnt.check(slice::from_ref(relation), &relations);
}

#[apply(generic_step)]
#[step(
    expr = r"{object_root_label} {var} get relations\({type_label}\) with role\({type_label}\) {contains_or_doesnt}: {var}"
)]
async fn object_get_relations_of_type_with_role_contain(
    context: &mut Context,
    object_root: params::ObjectRootLabel,
    player_var: params::Var,
    relation_type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    relation_var: params::Var,
) {
    let player = &context.objects.get(&player_var.name).unwrap().as_ref().unwrap().object;
    object_root.assert(&player.type_());
    let relations = with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(&tx.snapshot, &relation_type_label.into_typedb()).unwrap().unwrap();
        let role_type = relation_type
            .get_relates_role_name(&tx.snapshot, &tx.type_manager, role_label.into_typedb().name().as_str())
            .unwrap()
            .unwrap()
            .role();
        player
            .get_relations_by_role(&tx.snapshot, &tx.thing_manager, role_type)
            .map_static(|res| res.map(|(relation, count)| relation.into_owned()).unwrap())
            .collect::<Vec<_>>()
    });
    let Object::Relation(relation) = &context.objects.get(&relation_var.name).unwrap().as_ref().unwrap().object else {
        panic!()
    };
    contains_or_doesnt.check(slice::from_ref(relation), &relations);
}

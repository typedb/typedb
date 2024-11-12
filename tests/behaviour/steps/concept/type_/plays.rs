/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::type_::{annotation, constraint::Constraint, plays::PlaysAnnotation, Capability, PlayerAPI, TypeAPI};
use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;

use super::thing_type::get_as_object_type;
use crate::{
    generic_step, params,
    transaction_context::{with_read_tx, with_schema_tx},
    util, Context,
};

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set plays: {type_label}{may_error}")]
pub async fn set_plays(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let res = object_type.set_plays(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            role_type,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) unset plays: {type_label}{may_error}")]
pub async fn unset_plays(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let res = object_type.unset_plays(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            role_type,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays {contains_or_doesnt}:")]
pub async fn get_plays_contain(
    context: &mut Context,
    step: &Step,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_labels = object_type
            .get_plays(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|plays| {
                plays
                    .role()
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned()
            })
            .collect_vec();
        contains.check(&expected_labels, &actual_labels);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get declared plays {contains_or_doesnt}:")]
pub async fn get_declared_plays_contain(
    context: &mut Context,
    step: &Step,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_labels = object_type
            .get_plays_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|plays| {
                plays
                    .role()
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned()
            })
            .collect_vec();
        contains.check(&expected_labels, &actual_labels);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays {is_empty_or_not}")]
pub async fn get_plays_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_is_empty = object_type.get_plays(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get declared plays {is_empty_or_not}")]
pub async fn get_declared_plays_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_is_empty =
            object_type.get_plays_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) set annotation: {annotation}{may_error}")]
pub async fn get_plays_set_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    annotation: params::Annotation,
    may_error: params::MayError,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let res = plays.set_annotation(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            annotation.into_typedb(None).try_into().unwrap(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) unset annotation: {annotation_category}{may_error}"
)]
pub async fn get_plays_unset_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    annotation_category: params::AnnotationCategory,
    may_error: params::MayError,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let res = plays.unset_annotation(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            annotation_category.into_typedb(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get constraints for played role\\({type_label}\\) {contains_or_doesnt}: {constraint}"
)]
pub async fn get_constraints_for_played_role_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();

        let expected_constraint = constraint.into_typedb(None);
        let actual_contains = player_type
            .get_played_role_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, role_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get constraint categories for played role\\({type_label}\\) {contains_or_doesnt}: {constraint_category}"
)]
pub async fn get_constraint_categories_for_played_role_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = player_type
            .get_played_role_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, role_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get constraints {contains_or_doesnt}: {constraint}"
)]
pub async fn get_plays_constraints_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();

        let expected_constraint = constraint.into_typedb(None);
        let actual_contains = plays
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get constraint categories {contains_or_doesnt}: {constraint_category}"
)]
pub async fn get_plays_constraint_categories_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = plays
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get constraints {is_empty_or_not}")]
pub async fn get_owns_constraints_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_is_empty = plays.get_constraints(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get declared annotations {contains_or_doesnt}: {annotation}"
)]
pub async fn get_plays_declared_annotations_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation: params::Annotation,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_contains = plays
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .contains(&annotation.into_typedb(None).try_into().unwrap());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get declared annotation categories {contains_or_doesnt}: {annotation_category}"
)]
pub async fn get_plays_declared_annotation_categories_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation_category: params::AnnotationCategory,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_contains = plays
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|annotation| <PlaysAnnotation as Into<annotation::Annotation>>::into(annotation.clone()).category())
            .contains(&annotation_category.into_typedb());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get declared annotations {is_empty_or_not}")]
pub async fn get_owns_declared_annotations_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_is_empty =
            plays.get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get cardinality: {annotation}")]
pub async fn get_plays_cardinality(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    cardinality_annotation: params::Annotation,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_cardinality = plays.get_cardinality(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        match cardinality_annotation.into_typedb(None) {
            annotation::Annotation::Cardinality(card) => assert_eq!(actual_cardinality, card),
            _ => panic!("Expected annotations is not Cardinality"),
        }
    });
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::type_::{annotation, plays::PlaysAnnotation, Capability, PlayerAPI, TypeAPI};
use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;

use super::thing_type::get_as_object_type;
use crate::{concept::type_::BehaviourConceptTestExecutionError, generic_step, transaction_context::{with_read_tx, with_schema_tx}, util, Context, params};

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set plays: {type_label}{may_error}")]
pub async fn set_plays(context: &mut Context, kind: params::Kind, type_label: params::Label, role_label: params::Label, may_error: params::MayError) {
    let object_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
pub async fn unset_plays(context: &mut Context, kind: params::Kind, type_label: params::Label, role_label: params::Label, may_error: params::MayError) {
    let object_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
    let object_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
    let object_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
pub async fn get_plays_is_empty(context: &mut Context, kind: params::Kind, type_label: params::Label, is_empty_or_not: params::IsEmptyOrNot) {
    let object_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let actual_is_empty = object_type.get_plays(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) set override: {type_label}{may_error}")]
pub async fn get_plays_set_override(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    overridden_role_label: params::Label,
    may_error: params::MayError,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();

        if let Some(player_supertype) = player_type.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap() {
            let overridden_role_type = tx
                .type_manager
                .get_role_type(tx.snapshot.as_ref(), &overridden_role_label.into_typedb())
                .unwrap()
                .unwrap();
            let overridden_plays_opt =
                player_supertype.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, overridden_role_type).unwrap();

            if let Some(overridden_plays) = overridden_plays_opt {
                let res = plays.set_override(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                    overridden_plays,
                );
                may_error.check_concept_write_without_read_errors(&res);
                return;
            }
        }

        may_error.check::<(), BehaviourConceptTestExecutionError>(&Err(
            BehaviourConceptTestExecutionError::CannotFindObjectTypeRoleTypeToOverride,
        ));
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) unset override{may_error}")]
pub async fn get_plays_unset_override(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_schema_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let res = plays.unset_override(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.type_manager, &tx.thing_manager);
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays overridden\\({type_label}\\) {exists_or_doesnt}")]
pub async fn get_plays_overridden_exists(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    exists: params::ExistsOrDoesnt,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let plays_override_opt = plays.get_override(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        exists.check(
            &plays_override_opt,
            &format!("no plays override for {} of {}", role_label.into_typedb(), type_label.into_typedb()),
        );
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get plays overridden\\({type_label}\\) get label: {type_label}")]
pub async fn get_plays_overridden_get_label(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    expected_overridden: params::Label,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_overridden_opt = plays.get_override(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        let actual_overridden = actual_overridden_opt.as_ref().unwrap();
        let actual_overridden_label = actual_overridden
            .role()
            .get_label(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .scoped_name()
            .as_str()
            .to_owned();

        assert_eq!(expected_overridden.into_typedb().scoped_name().as_str().to_owned(), actual_overridden_label);
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
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get annotations {contains_or_doesnt}: {annotation}"
)]
pub async fn get_plays_annotations_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation: params::Annotation,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_contains = plays
            .get_annotations(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .contains_key(&annotation.into_typedb(None).try_into().unwrap());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get annotation categories {contains_or_doesnt}: {annotation_category}"
)]
pub async fn get_plays_annotation_categories_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation_category: params::AnnotationCategory,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_contains = plays
            .get_annotations(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|(annotation, _)| {
                <PlaysAnnotation as Into<annotation::Annotation>>::into(annotation.clone()).category()
            })
            .contains(&annotation_category.into_typedb());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
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
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
#[step(expr = "{kind}\\({type_label}\\) get plays\\({type_label}\\) get annotations {is_empty_or_not}")]
pub async fn get_owns_annotations_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
    with_read_tx!(context, |tx| {
        let role_type =
            tx.type_manager.get_role_type(tx.snapshot.as_ref(), &role_label.into_typedb()).unwrap().unwrap();
        let plays = player_type.get_plays_role(tx.snapshot.as_ref(), &tx.type_manager, role_type).unwrap().unwrap();
        let actual_is_empty = plays.get_annotations(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
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
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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
    let player_type = get_as_object_type(context, kind.into_typedb(), &type_label);
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

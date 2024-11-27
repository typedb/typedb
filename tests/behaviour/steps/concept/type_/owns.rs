/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::type_::{
    annotation, annotation::Annotation, constraint::Constraint, Capability, Ordering, OwnerAPI, TypeAPI,
};
use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;

use super::thing_type::get_as_object_type;
use crate::{
    generic_step, params,
    params::check_boolean,
    transaction_context::{with_read_tx, with_schema_tx},
    util, Context,
};

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set owns: {type_label}{may_error}")]
pub async fn set_owns_unordered(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attribute_type_label: params::Label,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        let res = object_type.set_owns(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            attr_type,
            Ordering::Unordered,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set owns: {type_label}[]{may_error}")]
pub async fn set_owns_ordered(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attribute_type_label: params::Label,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        let res = object_type.set_owns(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            attr_type,
            Ordering::Ordered,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) unset owns: {type_label}{may_error}")]
pub async fn unset_owns(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attribute_type_label: params::Label,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type = tx
            .type_manager
            .get_attribute_type(tx.snapshot.as_ref(), &attribute_type_label.into_typedb())
            .unwrap()
            .unwrap();
        let res = object_type.unset_owns(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            attr_type,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) set annotation: {annotation}{may_error}")]
pub async fn get_owns_set_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    annotation: params::Annotation,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let value_type = attr_type.get_value_type_without_source(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        let res = owns.set_annotation(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            annotation.into_typedb(value_type).try_into().unwrap(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) unset annotation: {annotation_category}{may_error}")]
pub async fn get_owns_unset_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    annotation_category: params::AnnotationCategory,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let res = owns.unset_annotation(
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
    expr = "{kind}\\({type_label}\\) get constraints for owned attribute\\({type_label}\\) {contains_or_doesnt}: {constraint}"
)]
pub async fn get_constraints_for_owned_attribute_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let value_type = attr_type.get_value_type_without_source(tx.snapshot.as_ref(), &tx.type_manager).unwrap();

        let expected_constraint = constraint.into_typedb(value_type);
        let actual_contains = object_type
            .get_owned_attribute_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, attr_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get constraint categories for owned attribute\\({type_label}\\) {contains_or_doesnt}: {constraint_category}"
)]
pub async fn get_constraint_categories_for_owned_attribute_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = object_type
            .get_owned_attribute_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, attr_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get constraints {contains_or_doesnt}: {constraint}")]
pub async fn get_owns_constraints_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let value_type =
            owns.attribute().get_value_type_without_source(tx.snapshot.as_ref(), &tx.type_manager).unwrap();

        let expected_constraint = constraint.into_typedb(value_type);
        let actual_contains = owns
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get constraint categories {contains_or_doesnt}: {constraint_category}"
)]
pub async fn get_owns_constraint_categories_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = owns
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get constraints {is_empty_or_not}")]
pub async fn get_owns_constraints_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();

        let actual_is_empty = owns.get_constraints(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get declared annotations {contains_or_doesnt}: {annotation}"
)]
pub async fn get_owns_declared_annotations_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation: params::Annotation,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let value_type =
            owns.attribute().get_value_type_without_source(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        let actual_contains = owns
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .contains(&annotation.into_typedb(value_type).try_into().unwrap());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get declared annotation categories {contains_or_doesnt}: {annotation_category}"
)]
pub async fn get_owns_declared_annotation_categories_contains(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation_category: params::AnnotationCategory,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();

        let parsed_annotation_category = annotation_category.into_typedb();
        let actual_contains = owns
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .map(|annotation| Annotation::from(annotation.clone()).category())
            .contains(&parsed_annotation_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get declared annotations {is_empty_or_not}")]
pub async fn get_owns_declared_annotations_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();

        let actual_is_empty = owns.get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get cardinality: {annotation}")]
pub async fn get_owns_cardinality(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    cardinality_annotation: params::Annotation,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let actual_cardinality = owns.get_cardinality(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        match cardinality_annotation.into_typedb(None) {
            annotation::Annotation::Cardinality(card) => assert_eq!(actual_cardinality, card),
            _ => panic!("Expected annotations is not Cardinality"),
        }
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns {contains_or_doesnt}:")]
pub async fn get_owns_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_labels = object_type
            .get_owns(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|owns| {
                owns.attribute()
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
#[step(expr = "{kind}\\({type_label}\\) get owns {is_empty_or_not}")]
pub async fn get_owns_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_is_empty = object_type.get_owns(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get declared owns {contains_or_doesnt}:")]
pub async fn get_declared_owns_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_labels = object_type
            .get_owns_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|owns| {
                owns.attribute()
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
#[step(expr = "{kind}\\({type_label}\\) get declared owns {is_empty_or_not}")]
pub async fn get_declared_owns_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let actual_is_empty = object_type.get_owns_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get label: {type_label}")]
pub async fn get_owns_get_label(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    expected_label: params::Label,
) {
    let owner = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = owner.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let actual_type_label = owns
            .attribute()
            .get_label(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .scoped_name()
            .as_str()
            .to_owned();
        assert_eq!(expected_label.into_typedb().scoped_name().as_str().to_owned(), actual_type_label);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) set ordering: {ordering}{may_error}")]
pub async fn get_owns_set_ordering(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    ordering: params::Ordering,
    may_error: params::MayError,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_schema_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        let res = owns.set_ordering(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            ordering.into_typedb(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) get ordering: {ordering}")]
pub async fn get_owns_get_ordering(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    ordering: params::Ordering,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        assert_eq!(owns.get_ordering(tx.snapshot.as_ref(), &tx.type_manager).unwrap(), ordering.into_typedb());
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get owns\\({type_label}\\) is key: {boolean}")]
pub async fn get_owns_is_key(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    attr_type_label: params::Label,
    is: params::Boolean,
) {
    let object_type = get_as_object_type(context, kind.into_typedb(), type_label);
    with_read_tx!(context, |tx| {
        let attr_type =
            tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &attr_type_label.into_typedb()).unwrap().unwrap();
        let owns = object_type.get_owns_attribute(tx.snapshot.as_ref(), &tx.type_manager, attr_type).unwrap().unwrap();
        check_boolean!(is, owns.is_key(tx.snapshot.as_ref(), &tx.type_manager).unwrap());
    });
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::type_::{
    annotation, attribute_type::AttributeTypeAnnotation, constraint::Constraint, entity_type::EntityTypeAnnotation,
    object_type::ObjectType, relation_type::RelationTypeAnnotation, KindAPI, TypeAPI,
};
use cucumber::gherkin::Step;
use encoding::{graph::type_::Kind, value::value_type::ValueType};
use itertools::Itertools;
use macro_rules_attribute::apply;
use params;

use crate::{
    generic_step,
    transaction_context::{with_read_tx, with_schema_tx, with_write_tx},
    util, with_type, Context,
};

#[macro_export]
macro_rules! with_type {
    ($tx:ident, $kind:expr, $label:ident, $assign_to:ident, $block:block) => {
        use encoding::graph::type_::Kind;
        match $kind.into_typedb() {
            Kind::Attribute => {
                let $assign_to = $tx.type_manager.get_attribute_type($tx.snapshot.as_ref(), &$label).unwrap().unwrap();
                $block
            }
            Kind::Entity => {
                let $assign_to = $tx.type_manager.get_entity_type($tx.snapshot.as_ref(), &$label).unwrap().unwrap();
                $block
            }
            Kind::Relation => {
                let $assign_to = $tx.type_manager.get_relation_type($tx.snapshot.as_ref(), &$label).unwrap().unwrap();
                $block
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    };
}

#[macro_export]
macro_rules! with_type_and_value_type {
    ($tx:ident, $kind:expr, $label:ident, $assign_type_to:ident, $assign_value_type_to:ident, $block:block) => {
        use encoding::graph::type_::Kind;
        let mut $assign_value_type_to: Option<ValueType> = None;
        match $kind.into_typedb() {
            Kind::Attribute => {
                let $assign_type_to =
                    $tx.type_manager.get_attribute_type($tx.snapshot.as_ref(), &$label.into_typedb()).unwrap().unwrap();
                $assign_value_type_to =
                    $assign_type_to.get_value_type_without_source($tx.snapshot.as_ref(), &$tx.type_manager).unwrap();
                $block
            }
            Kind::Entity => {
                let $assign_type_to =
                    $tx.type_manager.get_entity_type($tx.snapshot.as_ref(), &$label.into_typedb()).unwrap().unwrap();
                $block
            }
            Kind::Relation => {
                let $assign_type_to =
                    $tx.type_manager.get_relation_type($tx.snapshot.as_ref(), &$label.into_typedb()).unwrap().unwrap();
                $block
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    };
}

pub(super) fn get_as_object_type(context: &mut Context, kind: Kind, label: params::Label) -> ObjectType {
    with_read_tx!(context, |tx| {
        match kind {
            Kind::Entity => {
                let type_ =
                    tx.type_manager.get_entity_type(tx.snapshot.as_ref(), &label.into_typedb()).unwrap().unwrap();
                ObjectType::Entity(type_)
            }
            Kind::Relation => {
                let type_ =
                    tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &label.into_typedb()).unwrap().unwrap();
                ObjectType::Relation(type_)
            }
            _ => unreachable!("Attribute type as ObjectType is deprecated."),
        }
    })
}

#[apply(generic_step)]
#[step(expr = "create {kind} type: {type_label}{may_error}")]
pub async fn type_create(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        match kind.into_typedb() {
            Kind::Entity => {
                may_error.check_concept_write_without_read_errors(
                    &tx.type_manager
                        .create_entity_type(Arc::get_mut(&mut tx.snapshot).unwrap(), &type_label.into_typedb()),
                );
            }
            Kind::Relation => {
                may_error.check_concept_write_without_read_errors(
                    &tx.type_manager
                        .create_relation_type(Arc::get_mut(&mut tx.snapshot).unwrap(), &type_label.into_typedb()),
                );
            }
            Kind::Attribute => {
                may_error.check_concept_write_without_read_errors(
                    &tx.type_manager
                        .create_attribute_type(Arc::get_mut(&mut tx.snapshot).unwrap(), &type_label.into_typedb()),
                );
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        }
    });
}

#[apply(generic_step)]
#[step(expr = "delete {kind} type: {type_label}{may_error}")]
pub async fn type_delete(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    may_error: params::MayError,
) {
    let type_label = type_label.into_typedb();
    with_schema_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let res = type_.delete(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.type_manager, &tx.thing_manager);
            may_error.check_concept_write_without_read_errors(&res);
        });
    });
}

//
#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) {exists_or_doesnt}")]
pub async fn type_exists(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    exists: params::ExistsOrDoesnt,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        match kind.into_typedb() {
            Kind::Attribute => {
                let type_ = tx.type_manager.get_attribute_type(tx.snapshot.as_ref(), &type_label).unwrap();
                exists.check(&type_, &format!("type {}", type_label));
            }
            Kind::Entity => {
                let type_ = tx.type_manager.get_entity_type(tx.snapshot.as_ref(), &type_label).unwrap();
                exists.check(&type_, &format!("type {}", type_label));
            }
            Kind::Relation => {
                let type_ = tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label).unwrap();
                exists.check(&type_, &format!("type {}", type_label));
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set label: {type_label}{may_error}")]
pub async fn type_set_label(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    to_label: params::Label,
    may_error: params::MayError,
) {
    let type_label = type_label.into_typedb();
    with_schema_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            may_error.check_concept_write_without_read_errors(&type_.set_label(
                Arc::get_mut(&mut tx.snapshot).unwrap(),
                &tx.type_manager,
                &to_label.into_typedb(),
            ));
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get name: {type_label}")]
pub async fn type_get_name(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    expected: params::Label,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let actual_label = type_.get_label(tx.snapshot.as_ref(), &tx.type_manager);
            assert_eq!(expected.into_typedb().name(), actual_label.unwrap().name());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get label: {type_label}")]
pub async fn type_get_label(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    expected: params::Label,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let actual_label = type_.get_label(tx.snapshot.as_ref(), &tx.type_manager);
            assert_eq!(expected.into_typedb().scoped_name(), actual_label.unwrap().scoped_name());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set annotation: {annotation}{may_error}")]
pub async fn type_set_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    annotation: params::Annotation,
    may_error: params::MayError,
) {
    with_write_tx!(context, |tx| {
        with_type_and_value_type!(tx, kind, type_label, type_, value_type, {
            let res = type_.set_annotation(
                Arc::get_mut(&mut tx.snapshot).unwrap(),
                &tx.type_manager,
                &tx.thing_manager,
                annotation.into_typedb(value_type).try_into().unwrap(),
            );
            may_error.check_concept_write_without_read_errors(&res);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) unset annotation: {annotation_category}{may_error}")]
pub async fn type_unset_annotation(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    annotation_category: params::AnnotationCategory,
    may_error: params::MayError,
) {
    let type_label = type_label.into_typedb();
    with_write_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let res = type_.unset_annotation(
                Arc::get_mut(&mut tx.snapshot).unwrap(),
                &tx.type_manager,
                annotation_category.into_typedb(),
            );
            may_error.check_concept_write_without_read_errors(&res);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get constraints {contains_or_doesnt}: {constraint}")]
pub async fn type_constraints_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    with_read_tx!(context, |tx| {
        with_type_and_value_type!(tx, kind, type_label, type_, value_type, {
            let expected_constraint = constraint.into_typedb(value_type);
            let actual_contains = type_
                .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
                .unwrap()
                .into_iter()
                .any(|constraint| constraint.description() == expected_constraint);
            assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get constraint categories {contains_or_doesnt}: {constraint_category}")]
pub async fn type_constraint_categories_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let expected_constraint_category = constraint_category.into_typedb();
            let actual_contains = type_
                .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
                .unwrap()
                .into_iter()
                .any(|constraint| constraint.category() == expected_constraint_category);
            assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get constraints {is_empty_or_not}")]
pub async fn type_constraints_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let actual_is_empty = type_.get_constraints(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
            is_empty_or_not.check(actual_is_empty);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get declared annotations {contains_or_doesnt}: {annotation}")]
pub async fn type_declared_annotations_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation: params::Annotation,
) {
    with_read_tx!(context, |tx| {
        with_type_and_value_type!(tx, kind, type_label, type_, value_type, {
            let actual_contains = type_
                .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
                .unwrap()
                .contains(&annotation.into_typedb(value_type).try_into().unwrap());
            assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
        });
    });
}

#[apply(generic_step)]
#[step(
    expr = "{kind}\\({type_label}\\) get declared annotation categories {contains_or_doesnt}: {annotation_category}"
)]
pub async fn type_declared_annotation_categories_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation_category: params::AnnotationCategory,
) {
    with_read_tx!(context, |tx| {
        match kind.into_typedb() {
            Kind::Attribute => {
                let type_ = tx
                    .type_manager
                    .get_attribute_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let actual_contains = type_
                    .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .iter()
                    .map(|annotation| {
                        <AttributeTypeAnnotation as Into<annotation::Annotation>>::into(annotation.clone()).category()
                    })
                    .contains(&annotation_category.into_typedb());
                assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
            }
            Kind::Entity => {
                let type_ =
                    tx.type_manager.get_entity_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
                let actual_contains = type_
                    .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .iter()
                    .map(|annotation| {
                        <EntityTypeAnnotation as Into<annotation::Annotation>>::into(*annotation).category()
                    })
                    .contains(&annotation_category.into_typedb());
                assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
            }
            Kind::Relation => {
                let type_ = tx
                    .type_manager
                    .get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let actual_contains = type_
                    .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .iter()
                    .map(|annotation| {
                        <RelationTypeAnnotation as Into<annotation::Annotation>>::into(*annotation).category()
                    })
                    .contains(&annotation_category.into_typedb());
                assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get declared annotations {is_empty_or_not}")]
pub async fn type_declared_annotations_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let actual_is_empty =
                type_.get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
            is_empty_or_not.check(actual_is_empty);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) set supertype: {type_label}{may_error}")]
pub async fn type_set_supertype(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    supertype_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        match kind.into_typedb() {
            Kind::Attribute => {
                let thistype = tx
                    .type_manager
                    .get_attribute_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let supertype = tx
                    .type_manager
                    .get_attribute_type(tx.snapshot.as_ref(), &supertype_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let res = thistype.set_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                    supertype,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Entity => {
                let thistype =
                    tx.type_manager.get_entity_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
                let supertype = tx
                    .type_manager
                    .get_entity_type(tx.snapshot.as_ref(), &supertype_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let res = thistype.set_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                    supertype,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Relation => {
                let thistype = tx
                    .type_manager
                    .get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let supertype = tx
                    .type_manager
                    .get_relation_type(tx.snapshot.as_ref(), &supertype_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let res = thistype.set_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                    supertype,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) unset supertype{may_error}")]
pub async fn type_unset_supertype(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        match kind.into_typedb() {
            Kind::Attribute => {
                let thistype = tx
                    .type_manager
                    .get_attribute_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let res = thistype.unset_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Entity => {
                let thistype =
                    tx.type_manager.get_entity_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
                let res = thistype.unset_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Relation => {
                let thistype = tx
                    .type_manager
                    .get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb())
                    .unwrap()
                    .unwrap();
                let res = thistype.unset_supertype(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                );
                may_error.check_concept_write_without_read_errors(&res);
            }
            Kind::Role => unreachable!("Can only address roles through relation(relation_label) get role(role_name)"),
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get supertype: {type_label}")]
pub async fn type_get_supertype(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    supertype_label: params::Label,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let supertype = type_.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap().unwrap();
            assert_eq!(
                supertype_label.into_typedb().scoped_name(),
                supertype.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name()
            )
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get supertype {exists_or_doesnt}")]
pub async fn type_get_supertype_exists(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    exists: params::ExistsOrDoesnt,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let supertype = type_.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
            exists.check(&supertype, &format!("supertype for type {}", type_label));
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get supertypes {contains_or_doesnt}:")]
pub async fn get_supertypes_transitive_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let type_label = type_label.into_typedb();
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let supertype_labels = type_
                .get_supertypes_transitive(tx.snapshot.as_ref(), &tx.type_manager)
                .unwrap()
                .iter()
                .map(|supertype| {
                    supertype
                        .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                        .unwrap()
                        .scoped_name()
                        .as_str()
                        .to_owned()
                })
                .collect_vec();
            contains.check(&expected_labels, &supertype_labels);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get supertypes {is_empty_or_not}")]
pub async fn get_supertypes_transitive_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            is_empty_or_not
                .check(type_.get_supertypes_transitive(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get subtypes {contains_or_doesnt}:")]
pub async fn get_subtypes_contain(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let type_label = type_label.into_typedb();
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            let subtype_labels = type_
                .get_subtypes_transitive(tx.snapshot.as_ref(), &tx.type_manager)
                .unwrap()
                .iter()
                .map(|subtype| {
                    subtype.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec();
            contains.check(&expected_labels, &subtype_labels);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{kind}\\({type_label}\\) get subtypes {is_empty_or_not}")]
pub async fn get_subtypes_is_empty(
    context: &mut Context,
    kind: params::Kind,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    let type_label = type_label.into_typedb();
    with_read_tx!(context, |tx| {
        with_type!(tx, kind, type_label, type_, {
            is_empty_or_not
                .check(type_.get_subtypes_transitive(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "get {kind_extended} types {contains_or_doesnt}:")]
pub async fn get_types_contain(
    context: &mut Context,
    kind: params::KindExtended,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    let type_labels = with_read_tx!(context, |tx| {
        use params::KindExtended;
        match kind {
            KindExtended::Entity => &tx
                .type_manager
                .get_entity_types(tx.snapshot.as_ref())
                .unwrap()
                .into_iter()
                .map(|type_| {
                    type_.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec(),
            KindExtended::Relation => &tx
                .type_manager
                .get_relation_types(tx.snapshot.as_ref())
                .unwrap()
                .into_iter()
                .map(|type_| {
                    type_.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec(),
            KindExtended::Attribute => &tx
                .type_manager
                .get_attribute_types(tx.snapshot.as_ref())
                .unwrap()
                .into_iter()
                .map(|type_| {
                    type_.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec(),
            KindExtended::Role => &tx
                .type_manager
                .get_role_types(tx.snapshot.as_ref())
                .unwrap()
                .into_iter()
                .map(|type_| {
                    type_.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec(),
            KindExtended::Object => &tx
                .type_manager
                .get_object_types(tx.snapshot.as_ref())
                .unwrap()
                .into_iter()
                .map(|type_| {
                    type_.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
                })
                .collect_vec(),
        }
    });
    contains.check(&expected_labels, type_labels)
}

#[apply(generic_step)]
#[step(expr = "get {kind_extended} types {is_empty_or_not}")]
pub async fn get_types_empty(context: &mut Context, kind: params::KindExtended, is_empty_or_not: params::IsEmptyOrNot) {
    let is_empty = with_read_tx!(context, |tx| {
        use params::KindExtended;
        match kind {
            KindExtended::Entity => &tx.type_manager.get_entity_types(tx.snapshot.as_ref()).unwrap().is_empty(),
            KindExtended::Relation => &tx.type_manager.get_relation_types(tx.snapshot.as_ref()).unwrap().is_empty(),
            KindExtended::Attribute => &tx.type_manager.get_attribute_types(tx.snapshot.as_ref()).unwrap().is_empty(),
            KindExtended::Role => &tx.type_manager.get_role_types(tx.snapshot.as_ref()).unwrap().is_empty(),
            KindExtended::Object => &tx.type_manager.get_object_types(tx.snapshot.as_ref()).unwrap().is_empty(),
        }
    });
    is_empty_or_not.check(*is_empty)
}

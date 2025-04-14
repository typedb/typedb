/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::type_::{
    annotation::Annotation, constraint::Constraint, object_type::ObjectType, Capability, KindAPI, Ordering, TypeAPI,
};
use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;
use params::{self, check_boolean};

use crate::{
    concept::type_::BehaviourConceptTestExecutionError,
    generic_step,
    transaction_context::{with_read_tx, with_schema_tx},
    util, Context,
};

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) create role: {type_label}{may_error}")]
pub async fn relation_type_create_role_unordered(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    let res = with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        relation_type.create_relates(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            role_label.into_typedb().name().as_str(),
            Ordering::Unordered,
        )
    });
    may_error.check_concept_write_without_read_errors(&res);
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) create role: {type_label}[]{may_error}")]
pub async fn relation_type_create_role_ordered(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    let res = with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        relation_type.create_relates(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            role_label.into_typedb().name().as_str(),
            Ordering::Ordered,
        )
    });
    may_error.check_concept_write_without_read_errors(&res);
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) set specialise: {type_label}{may_error}")]
pub async fn relation_role_set_specialise(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    superrole_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        if let Some(relation_supertype) = relation_type.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap() {
            if let Some(specialised_relates) = relation_supertype
                .get_relates_role_name(
                    tx.snapshot.as_ref(),
                    &tx.type_manager,
                    superrole_label.into_typedb().name().as_str(),
                )
                .unwrap()
            {
                let res = relates.set_specialise(
                    Arc::get_mut(&mut tx.snapshot).unwrap(),
                    &tx.type_manager,
                    &tx.thing_manager,
                    specialised_relates,
                );
                may_error.check_concept_write_without_read_errors(&res);
                return;
            }
        }
        may_error.check::<(), BehaviourConceptTestExecutionError>(Err(
            BehaviourConceptTestExecutionError::CannotFindRelationTypeRoleTypeToSpecialise,
        ));
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) unset specialise{may_error}")]
pub async fn relation_role_unset_specialise(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        let res =
            relates.unset_specialise(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.type_manager, &tx.thing_manager);
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get relates {contains_or_doesnt}:")]
pub async fn relation_roles_contain(
    context: &mut Context,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect::<Vec<String>>();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = relation_type
            .get_relates(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
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
#[step(expr = r"relation\({type_label}\) get relates {is_empty_or_not}")]
pub async fn relation_roles_is_empty(
    context: &mut Context,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = relation_type
            .get_relates(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
                    .role()
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned()
            })
            .collect_vec();
        is_empty_or_not.check(actual_labels.is_empty());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get declared relates {contains_or_doesnt}:")]
pub async fn relation_declared_roles_contain(
    context: &mut Context,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect::<Vec<String>>();
    with_read_tx!(context, |tx| {
        let type_ =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = type_
            .get_relates_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
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
#[step(expr = r"relation\({type_label}\) get explicit declared relates {contains_or_doesnt}:")]
pub async fn relation_explicit_declared_roles_contain(
    context: &mut Context,
    type_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect::<Vec<String>>();
    with_read_tx!(context, |tx| {
        let type_ =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = type_
            .get_relates_explicit_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
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
#[step(expr = r"relation\({type_label}\) get declared relates {is_empty_or_not}")]
pub async fn relation_declared_roles_is_empty(
    context: &mut Context,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let type_ =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = type_
            .get_relates_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
                    .role()
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned()
            })
            .collect_vec();
        is_empty_or_not.check(actual_labels.is_empty());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get explicit declared relates {is_empty_or_not}")]
pub async fn relation_explicit_declared_roles_is_empty(
    context: &mut Context,
    type_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let type_ =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let actual_labels = type_
            .get_relates_explicit_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|relates| {
                relates
                    .role()
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned()
            })
            .collect_vec();
        is_empty_or_not.check(actual_labels.is_empty());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) {exists_or_doesnt}")]
pub async fn relation_role_exists(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    exists: params::ExistsOrDoesnt,
) {
    let type_label = type_label.into_typedb();
    let role_label = role_label.into_typedb();
    with_read_tx!(context, |tx| {
        let relation_type = tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label).unwrap().unwrap();
        let relates_opt = relation_type
            .get_relates_role_name_declared(tx.snapshot.as_ref(), &tx.type_manager, role_label.name.as_str())
            .unwrap();
        exists.check(&relates_opt, &format!("role {}:{}", type_label, role_label));
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get relates\({type_label}\) is implicit: {boolean}")]
pub async fn relation_role_is_implicit(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    is: params::Boolean,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name(tx.snapshot.as_ref(), &tx.type_manager, role_label.into_typedb().name.as_str())
            .unwrap()
            .unwrap();
        check_boolean!(is, relates.is_implicit(tx.snapshot.as_ref(), &tx.type_manager).unwrap())
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get label: {type_label}")]
pub async fn relation_role_get_label(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    expected_label: params::Label,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name.as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        assert_eq!(
            expected_label.into_typedb().scoped_name.as_str(),
            role.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name.as_str()
        );
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get name: {type_label}")]
pub async fn relation_role_get_name(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    expected_label: params::Label,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name.as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        assert_eq!(
            expected_label.into_typedb().name.as_str(),
            role.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().name.as_str()
        );
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) delete role: {type_label}{may_error}")]
pub async fn relation_type_delete_role(
    context: &mut Context,
    type_label: params::Label,
    role_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &type_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name.as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let res = role.delete(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.type_manager, &tx.thing_manager);
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get supertype: {type_label}")]
pub async fn relation_role_get_supertype(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    expected_superrole_label: params::Label,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        let role = relates.role();
        let superrole = role.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap().unwrap();
        assert_eq!(
            expected_superrole_label.into_typedb().scoped_name(),
            superrole.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name()
        )
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get supertype {exists_or_doesnt}")]
pub async fn relation_role_get_supertype_exists(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    exists: params::ExistsOrDoesnt,
) {
    let role_label = role_label.into_typedb();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(tx.snapshot.as_ref(), &tx.type_manager, role_label.name().as_str())
            .unwrap()
            .unwrap()
            .role();
        let superrole = role.get_supertype(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        exists.check(&superrole, &format!("superrole for role type {}", role_label));
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get supertypes {is_empty_or_not}")]
pub async fn relation_role_supertypes_is_empty(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let is_empty = role.get_supertypes_transitive(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        is_empty_or_not.check(is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get supertypes {contains_or_doesnt}:")]
pub async fn relation_role_supertypes_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let supertype_labels = role
            .get_supertypes_transitive(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|supertype| {
                supertype.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
            })
            .collect_vec();
        contains.check(&expected_labels, &supertype_labels);
    });
}

// TODO: Make different transitive / non-transitive steps?
#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get subtypes {contains_or_doesnt}:")]
pub async fn relation_role_subtypes_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let subtype_labels = role
            .get_subtypes_transitive(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|subtype| {
                subtype.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
            })
            .collect_vec();
        contains.check(&expected_labels, &subtype_labels);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get subtypes {is_empty_or_not}")]
pub async fn relation_role_subtypes_is_empty(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let subtype_labels = role
            .get_subtypes_transitive(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|subtype| {
                subtype.get_label(tx.snapshot.as_ref(), &tx.type_manager).unwrap().scoped_name().as_str().to_owned()
            })
            .collect_vec();
        is_empty_or_not.check(subtype_labels.is_empty());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) set name: {type_label}{may_error}")]
pub async fn relation_role_set_name(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    to_label: params::Label,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let res = role.set_name(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            to_label.into_typedb().name.as_str(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) set annotation: {annotation}{may_error}")]
pub async fn relation_role_set_annotation(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    annotation: params::Annotation,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let parsed_annotation = annotation.into_typedb(None);
        let res = relates.set_annotation(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            parsed_annotation.try_into().unwrap(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) unset annotation: {annotation_category}{may_error}")]
pub async fn relation_role_unset_annotation(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    annotation_category: params::AnnotationCategory,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let parsed_annotation_category = annotation_category.into_typedb();
        let res = relates.unset_annotation(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            parsed_annotation_category,
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(
    expr = r"relation\({type_label}\) get constraints for related role\({type_label}\) {contains_or_doesnt}: {constraint}"
)]
pub async fn relation_get_constraints_for_related_role_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let typedb_role_label = role_label.into_typedb();
        assert!(typedb_role_label.scope.is_some(), "Expected scoped label of a role in this test method");
        let role_type = tx.type_manager.get_role_type(tx.snapshot.as_ref(), &typedb_role_label).unwrap().unwrap();

        let expected_constraint = constraint.into_typedb(None);
        let actual_contains = relation_type
            .get_related_role_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, role_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = r"relation\({type_label}\) get constraint categories for related role\({type_label}\) {contains_or_doesnt}: {constraint_category}"
)]
pub async fn relation_constraint_categories_for_related_role_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let typedb_role_label = role_label.into_typedb();
        assert!(typedb_role_label.scope.is_some(), "Expected scoped label of a role in this test method");
        let role_type = tx.type_manager.get_role_type(tx.snapshot.as_ref(), &typedb_role_label).unwrap().unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = relation_type
            .get_related_role_type_constraints(tx.snapshot.as_ref(), &tx.type_manager, role_type)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get constraints {contains_or_doesnt}: {constraint}")]
pub async fn relation_role_constraints_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint: params::Constraint,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let expected_constraint = constraint.into_typedb(None);
        let actual_contains = relates
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.description() == expected_constraint);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = r"relation\({type_label}\) get role\({type_label}\) get constraint categories {contains_or_doesnt}: {constraint_category}"
)]
pub async fn relation_role_constraint_categories_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    constraint_category: params::ConstraintCategory,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let expected_constraint_category = constraint_category.into_typedb();
        let actual_contains = relates
            .get_constraints(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .any(|constraint| constraint.category() == expected_constraint_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get constraints {is_empty_or_not}")]
pub async fn relation_role_constraints_is_empty(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        let relates_empty = relates.get_constraints(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        let role_empty = relates.role().get_constraints(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();

        let actual_is_empty = relates_empty && role_empty;
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(
    expr = r"relation\({type_label}\) get role\({type_label}\) get declared annotations {contains_or_doesnt}: {annotation}"
)]
pub async fn relation_role_declared_annotations_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation: params::Annotation,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let parsed_annotation = annotation.into_typedb(None);
        let actual_contains = relates
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .contains(&parsed_annotation.try_into().unwrap());
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(
    expr = r"relation\({type_label}\) get role\({type_label}\) get declared annotation categories {contains_or_doesnt}: {annotation_category}"
)]
pub async fn relation_role_declared_annotation_categories_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    annotation_category: params::AnnotationCategory,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();

        let parsed_annotation_category = annotation_category.into_typedb();
        let actual_contains = relates
            .get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .into_iter()
            .map(|annotation| Annotation::from(*annotation).category())
            .contains(&parsed_annotation_category);
        assert_eq!(contains_or_doesnt.expected_contains(), actual_contains);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get declared annotations {is_empty_or_not}")]
pub async fn relation_role_declared_annotations_is_empty(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    is_empty_or_not: params::IsEmptyOrNot,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        let relates_empty =
            relates.get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();
        let role_empty =
            relates.role().get_annotations_declared(tx.snapshot.as_ref(), &tx.type_manager).unwrap().is_empty();

        let actual_is_empty = relates_empty && role_empty;
        is_empty_or_not.check(actual_is_empty);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get cardinality: {annotation}")]
pub async fn relation_role_cardinality(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    cardinality_annotation: params::Annotation,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let relates = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap();
        let actual_cardinality = relates.get_cardinality(tx.snapshot.as_ref(), &tx.type_manager).unwrap();
        match cardinality_annotation.into_typedb(None) {
            Annotation::Cardinality(card) => assert_eq!(actual_cardinality, card),
            _ => panic!("Expected annotations is not Cardinality"),
        }
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) set ordering: {ordering}{may_error}")]
pub async fn relation_role_set_ordering(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    ordering: params::Ordering,
    may_error: params::MayError,
) {
    with_schema_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let res = role.set_ordering(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            ordering.into_typedb(),
        );
        may_error.check_concept_write_without_read_errors(&res);
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get ordering: {ordering}")]
pub async fn relation_role_get_ordering(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    ordering: params::Ordering,
) {
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        assert_eq!(role.get_ordering(tx.snapshot.as_ref(), &tx.type_manager).unwrap(), ordering.into_typedb());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get players {contains_or_doesnt}:")]
pub async fn role_type_players_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect::<Vec<String>>();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let actual_labels = role
            .get_player_types(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|(_, plays)| match plays.player() {
                ObjectType::Entity(entity_type) => entity_type
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned(),
                ObjectType::Relation(relation_type) => relation_type
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            })
            .collect::<Vec<String>>();
        contains_or_doesnt.check(expected_labels.as_slice(), actual_labels.as_slice());
    });
}

#[apply(generic_step)]
#[step(expr = r"relation\({type_label}\) get role\({type_label}\) get declared players {contains_or_doesnt}:")]
pub async fn role_type_declared_players_contain(
    context: &mut Context,
    relation_label: params::Label,
    role_label: params::Label,
    contains_or_doesnt: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| str.to_owned()).collect::<Vec<String>>();
    with_read_tx!(context, |tx| {
        let relation_type =
            tx.type_manager.get_relation_type(tx.snapshot.as_ref(), &relation_label.into_typedb()).unwrap().unwrap();
        let role = relation_type
            .get_relates_role_name_declared(
                tx.snapshot.as_ref(),
                &tx.type_manager,
                role_label.into_typedb().name().as_str(),
            )
            .unwrap()
            .unwrap()
            .role();
        let actual_labels = role
            .get_plays(tx.snapshot.as_ref(), &tx.type_manager)
            .unwrap()
            .iter()
            .map(|plays| match plays.player() {
                ObjectType::Entity(entity_type) => entity_type
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned(),
                ObjectType::Relation(relation_type) => relation_type
                    .get_label(tx.snapshot.as_ref(), &tx.type_manager)
                    .unwrap()
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            })
            .collect::<Vec<String>>();
        contains_or_doesnt.check(expected_labels.as_slice(), actual_labels.as_slice());
    });
}

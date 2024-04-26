use cucumber::gherkin::Step;
use macro_rules_attribute::apply;
use crate::{generic_step, tx_as_read, tx_as_write, tx_as_schema, concept, Context, params::{Boolean, MayError, ContainsOrDoesnt, RootLabel, Label}, transaction_context::{ActiveTransaction}, util};
use ::concept::type_::{
    TypeAPI, OwnerAPI,
    annotation::AnnotationAbstract,
    attribute_type::AttributeTypeAnnotation,
    entity_type::EntityTypeAnnotation,
    relation_type::RelationTypeAnnotation,
    role_type::RoleTypeAnnotation,
    object_type::ObjectType,
    Ordering
};
use encoding::graph::type_::Kind;
use crate::params::{OptionalOverride, WithAnnotations};

macro_rules! with_type {
    ($tx:ident, $kind:ident, $label:ident, $assign_to:ident, $block:block) => {
        match $kind.to_typedb() {
            Kind::Attribute => {
                let $assign_to = $tx.type_manager().get_attribute_type(&$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Entity => {
                let $assign_to = $tx.type_manager().get_entity_type(&$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Relation => {
                let $assign_to = $tx.type_manager().get_relation_type(&$label.to_typedb()).unwrap().unwrap();
                $block
            },
            Kind::Role => unreachable!(),
        };
    };
}

fn with_object_type(tx: &ActiveTransaction, kind: Kind, label: &Label) -> ObjectType<'static> {
    tx_as_read! (tx, {
        match kind {
            Kind::Entity => {
                let type_ = tx.type_manager().get_entity_type(&label.to_typedb()).unwrap().unwrap();
                return ObjectType::Entity(type_)
            },
            Kind::Relation => {
                let type_ = tx.type_manager().get_relation_type(&label.to_typedb()).unwrap().unwrap();
                return ObjectType::Relation(type_)
            },
            _ => unreachable!(),
        };
    })
}

#[apply(generic_step)]
#[step(expr = "put {root_label} type: {type_label}")]
pub async fn type_put(context: &mut Context, root_label: RootLabel, type_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_schema!(tx, {
        match root_label.to_typedb() {
            Kind::Entity => { tx.type_manager().create_entity_type(&type_label.to_typedb(), false).unwrap(); },
            Kind::Relation => { tx.type_manager().create_relation_type(&type_label.to_typedb(), false).unwrap(); },
            Kind::Attribute => unreachable!("Attribute type should have its own step"),
            Kind::Role => unreachable!(),
        }
    });
}
//
#[apply(generic_step)]
#[step(expr = "delete {root_label} type: {type_label}(; ){may_error}")]
pub async fn type_delete(context: &mut Context, root_label: RootLabel, type_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        with_type! (tx, root_label, type_label, type_, {
            let res = type_.delete(tx.type_manager());
            may_error.check(&res);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) is null: {boolean}")]
pub async fn type_is_null(context: &mut Context, root_label: RootLabel, type_label: Label, is_null: Boolean) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        match root_label.to_typedb() {
            Kind::Attribute => {
                let type_ = tx.type_manager().get_attribute_type(&type_label.to_typedb()).unwrap();
                is_null.check(type_.is_none());
            },
            Kind::Entity => {
                let type_ = tx.type_manager().get_entity_type(&type_label.to_typedb()).unwrap();
                is_null.check(type_.is_none());
            },
            Kind::Relation => {
                let type_ = tx.type_manager().get_relation_type(&type_label.to_typedb()).unwrap();
                is_null.check(type_.is_none());
            },
            Kind::Role => {
                let type_ = tx.type_manager().get_role_type(&type_label.to_typedb()).unwrap();
                is_null.check(type_.is_none());
            },
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set label: {type_label}")]
pub async fn type_set_label(context: &mut Context, root_label: RootLabel, type_label: Label, to_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            type_.set_label(tx.type_manager(), &to_label.to_typedb()).unwrap()
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get label: {type_label}")]
pub async fn type_get_label(context: &mut Context, root_label: RootLabel, type_label: Label, expected: Label) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let actual_label = type_.get_label(tx.type_manager());
            assert_eq!(expected.to_typedb().scoped_name(), actual_label.unwrap().scoped_name());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set abstract: {boolean}(; ){may_error}")]
pub async fn type_set_abstract(context: &mut Context, root_label: RootLabel, type_label: Label, set_abstract_to: Boolean, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_write! (tx, {
        match root_label.to_typedb() {
            Kind::Attribute => {
                let type_ = tx.type_manager().get_attribute_type(&type_label.to_typedb()).unwrap().unwrap();
                let res = type_.set_annotation(tx.type_manager(), AttributeTypeAnnotation::Abstract(AnnotationAbstract::new()));
                may_error.check(&res);
            },
            Kind::Entity => {
                let type_ = tx.type_manager().get_entity_type(&type_label.to_typedb()).unwrap().unwrap();
                let res = type_.set_annotation(tx.type_manager(), EntityTypeAnnotation::Abstract(AnnotationAbstract::new()));
                may_error.check(&res);
            },
            Kind::Relation => {
                let type_ = tx.type_manager().get_relation_type(&type_label.to_typedb()).unwrap().unwrap();
                let res = type_.set_annotation(tx.type_manager(), RelationTypeAnnotation::Abstract(AnnotationAbstract::new()));
                may_error.check(&res);
            },
            Kind::Role => {
                let type_ = tx.type_manager().get_role_type(&type_label.to_typedb()).unwrap().unwrap();
                let res = type_.set_annotation(tx.type_manager(), RoleTypeAnnotation::Abstract(AnnotationAbstract::new()));
                may_error.check(&res);
            },
        };
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) is abstract: {boolean}")]
pub async fn type_is_abstract(context: &mut Context, root_label: RootLabel, type_label: Label, is_abstract: Boolean) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let actual = type_.is_abstract(tx.type_manager()).unwrap();
            is_abstract.check(actual);
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set supertype: {type_label}(; ){may_error}")]
pub async fn type_set_supertype(context: &mut Context, root_label: RootLabel, type_label: Label, supertype_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    tx_as_schema! (tx, {
        match root_label.to_typedb() {
            Kind::Attribute => {
                let thistype = tx.type_manager().get_attribute_type(&type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager().get_attribute_type(&supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(tx.type_manager(), supertype);
                may_error.check(&res);
            },
            Kind::Entity => {
                let thistype = tx.type_manager().get_entity_type(&type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager().get_entity_type(&supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(tx.type_manager(), supertype);
                may_error.check(&res);
            },
            Kind::Relation => {
                let thistype = tx.type_manager().get_relation_type(&type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager().get_relation_type(&supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(tx.type_manager(), supertype);
                may_error.check(&res);
            },
            Kind::Role => {
                let thistype = tx.type_manager().get_role_type(&type_label.to_typedb()).unwrap().unwrap();
                let supertype = tx.type_manager().get_role_type(&supertype_label.to_typedb()).unwrap().unwrap();
                let res = thistype.set_supertype(tx.type_manager(), supertype);
                may_error.check(&res);
            },
        };
    });
}
//
#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get supertype: {type_label}")]
pub async fn type_get_supertype(context: &mut Context, root_label: RootLabel, type_label: Label, supertype_label: Label) {
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let supertype = type_.get_supertype(tx.type_manager()).unwrap().unwrap();
            assert_eq!(supertype_label.to_typedb().scoped_name(), supertype.get_label(tx.type_manager()).unwrap().scoped_name())
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get supertypes {contains_or_doesnt}:")]
pub async fn get_supertypes_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    tx_as_read! (tx, {
        with_type!(tx, root_label, type_label, type_, {
            let supertype_labels: Vec<String> = type_.get_supertypes(tx.type_manager())
            .unwrap().iter().map(|supertype| { supertype.get_label(tx.type_manager()).unwrap().scoped_name().as_str().to_string() })
            .collect::<Vec<String>>();
            contains.check(expected_labels, supertype_labels.into_iter().collect());
        });
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get subtypes {contains_or_doesnt}:")]
pub async fn get_subtypes_contain(context: &mut Context, root_label: RootLabel, type_label: Label, contains: ContainsOrDoesnt, step: &Step) {
    todo!();
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) set owns attribute type: {type_label}{optional_override}(, ){with_annotations}(; ){may_error}")]
pub async fn set_owns(context: &mut Context, root_label: RootLabel, type_label: Label, attribute_type_label: Label, optional_override: OptionalOverride, with_annotations: WithAnnotations, may_error: MayError) {
    let tx = context.transaction().unwrap();
    let object_type = with_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let attr_type = tx.type_manager().get_attribute_type(&attribute_type_label.to_typedb()).unwrap().unwrap();
        let owns = object_type.set_owns(tx.type_manager(), attr_type, Ordering::Unordered);
        // todo!("Set override");
        // // todo!("Set annotations");
        // for owns_annotation in with_annotations.to_owns().iter() {
        //     owns.set_annotation(tx.type_manager(), *owns_annotation);
        // }
        // todo!("Error handling of may_error");
    });
}


#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) unset owns attribute type: {type_label}(; ){may_error}")]
pub async fn unset_owns(context: &mut Context, root_label: RootLabel, type_label: Label, attribute_type_label: Label, may_error: MayError) {
    let tx = context.transaction().unwrap();
    let object_type = with_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_schema! (tx, {
        let attr_type = tx.type_manager().get_attribute_type(&attribute_type_label.to_typedb()).unwrap().unwrap();
        let res = object_type.delete_owns(tx.type_manager(), attr_type);
        // todo!("Error handling of may_error");
    });
}

#[apply(generic_step)]
#[step(expr = "{root_label}\\({type_label}\\) get owns attribute types(, ){with_annotations}(; ){contains_or_doesnt}:")]
pub async fn get_owns(context: &mut Context, root_label: RootLabel, type_label: Label, with_annotations: WithAnnotations, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels: Vec<String> = util::iter_table(step).map(|str| { str.to_string() }).collect::<Vec<String>>();
    let tx = context.transaction().unwrap();
    let object_type = with_object_type(tx, root_label.to_typedb(), &type_label);
    tx_as_read! (tx, {
        let actual_labels = object_type.get_owns(tx.type_manager()).unwrap().iter().map(|owns| {
            owns.attribute().get_label(tx.type_manager()).unwrap().scoped_name().as_str().to_string()
        }).collect::<Vec<String>>();
        contains.check(expected_labels, actual_labels);
        // todo!("Error handling of may_error");
    });
}

// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns attribute types, with annotations: {annotations}; do not contain:")]
// pub async fn get_owns_explicit(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types, with annotations: {annotations}; contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types, with annotations: {annotations}; do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }

// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns overridden attribute\\({type_label}\\) is null: {boolean}")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns overridden attribute\\({type_label}\\) get label: {type_label}")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns attribute types contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns attribute types do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get owns explicit attribute types do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) set plays role: {scoped_label}")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) set plays role: {scoped_label}; throws exception")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) set plays role: {scoped_label} as {scoped_label}")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) set plays role: {scoped_label} as {scoped_label}; throws exception")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) unset plays role: {scoped_label}")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) unset plays role: {scoped_label}; throws exception")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get playing roles contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get playing roles do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get playing roles explicit contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "{root_label}\\({type_label}\\) get playing roles explicit do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//


// TODO: thing type root - Deprecated?

// #[apply(generic_step)]
// #[step(expr = "thing type root get supertypes contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "thing type root get supertypes do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "thing type root get subtypes contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }
//
//
// #[apply(generic_step)]
// #[step(expr = "thing type root get subtypes do not contain:")]
// pub async fn TODO(context: &mut Context, root_label: RootLabel, type_label: Label, ...) { todo!(); }

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use answer::{Type, variable_value::VariableValue};
use compiler::VariablePosition;
use concept::{
    error::ConceptReadError,
    type_::{
        Capability, OwnerAPI, PlayerAPI, TypeAPI,
        annotation::{Annotation, AnnotationCategory, AnnotationMeta},
        attribute_type::AttributeTypeAnnotation,
        entity_type::EntityTypeAnnotation,
        owns::{Owns, OwnsAnnotation},
        plays::{Plays, PlaysAnnotation},
        relates::{Relates, RelatesAnnotation},
        relation_type::RelationTypeAnnotation,
        sub::{Sub, SubAnnotation},
    },
};
use encoding::value::value::Value;
use ir::translation::function::FunctionAnnotation;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{Provenance, batch::FixedBatch, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};

pub(crate) fn iid(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    _context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // all concepts have IIDs
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let iid = input_row[argument_positions[0].as_usize()].as_thing().iid();
    row[return_position.as_usize()] = VariableValue::Value(Value::String(Cow::Owned(format!("{iid:x}"))));
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn label(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // all types have labels
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let ty = input_row[argument_positions[0].as_usize()].as_type();
    let label = ty.get_label(&**context.snapshot(), context.type_manager())?;
    row[return_position.as_usize()] = VariableValue::Value(Value::String(Cow::Owned(label.to_string())));
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn get_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    row[return_position.as_usize()] = get_type_doc(context, &input_row[argument_positions[0].as_usize()])?;
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn get_owns_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let owner = &input_row[argument_positions[0].as_usize()];
    let attribute = &input_row[argument_positions[1].as_usize()];
    if let Some(owns) = get_owns(context, owner, attribute)? {
        row[return_position.as_usize()] = unwrap_doc(context.type_manager().get_owns_annotation_declared_by_category(
            &**context.snapshot(),
            owns,
            &AnnotationCategory::Doc,
        )?);
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_plays_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let player = &input_row[argument_positions[0].as_usize()];
    let role = &input_row[argument_positions[1].as_usize()];
    if let Some(plays) = get_plays(context, player, role)? {
        row[return_position.as_usize()] =
            unwrap_doc(context.type_manager().get_plays_annotation_declared_by_category(
                &**context.snapshot(),
                plays,
                &AnnotationCategory::Doc,
            )?);
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_relates_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let relation = &input_row[argument_positions[0].as_usize()];
    let role = &input_row[argument_positions[1].as_usize()];
    if let Some(relates) = get_relates(context, relation, role)? {
        row[return_position.as_usize()] =
            unwrap_doc(context.type_manager().get_relates_annotation_declared_by_category(
                &**context.snapshot(),
                relates,
                &AnnotationCategory::Doc,
            )?);
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_sub_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let subtype = &input_row[argument_positions[0].as_usize()];
    let supertype = &input_row[argument_positions[1].as_usize()];
    if let Some(a) = get_subtype_doc(context, subtype, supertype)? {
        row[return_position.as_usize()] = a;
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    row[return_position.as_usize()] = get_type_meta(
        context,
        &input_row[argument_positions[0].as_usize()],
        &input_row[argument_positions[1].as_usize()],
    )?;
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn get_owns_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let key = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    let owner = &input_row[argument_positions[1].as_usize()];
    let attribute = &input_row[argument_positions[2].as_usize()];
    if let Some(owns) = get_owns(context, owner, attribute)? {
        row[return_position.as_usize()] = unwrap_meta_value(
            context.type_manager().get_owns_annotation_declared_by_category(&**context.snapshot(), owns, category)?,
        );
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_plays_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let key = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    let player = &input_row[argument_positions[1].as_usize()];
    let role = &input_row[argument_positions[2].as_usize()];
    if let Some(plays) = get_plays(context, player, role)? {
        row[return_position.as_usize()] = unwrap_meta_value(
            context.type_manager().get_plays_annotation_declared_by_category(&**context.snapshot(), plays, category)?,
        );
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_relates_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let key = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    let relation = &input_row[argument_positions[1].as_usize()];
    let role = &input_row[argument_positions[2].as_usize()];
    if let Some(relates) = get_relates(context, relation, role)? {
        row[return_position.as_usize()] =
            unwrap_meta_value(context.type_manager().get_relates_annotation_declared_by_category(
                &**context.snapshot(),
                relates,
                category,
            )?);
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_sub_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let key = &input_row[argument_positions[0].as_usize()];
    let subtype = &input_row[argument_positions[1].as_usize()];
    let supertype = &input_row[argument_positions[2].as_usize()];
    if let Some(variable_value) = get_subtype_meta(context, key, subtype, supertype)? {
        row[return_position.as_usize()] = variable_value;
        let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn get_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let metas = get_type_all_meta(context, &input_row[argument_positions[0].as_usize()])?;
    put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)
}

pub(crate) fn get_owns_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let owner = &input_row[argument_positions[0].as_usize()];
    let attribute = &input_row[argument_positions[1].as_usize()];
    if let Some(owns) = get_owns(context, owner, attribute)? {
        let metas = get_owns_meta_annotations(context, owns)?;
        put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)?;
    }
    Ok(())
}

pub(crate) fn get_plays_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let player = &input_row[argument_positions[0].as_usize()];
    let role = &input_row[argument_positions[1].as_usize()];
    if let Some(plays) = get_plays(context, player, role)? {
        let metas = get_plays_meta_annotations(context, plays)?;
        put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)?;
    }
    Ok(())
}

pub(crate) fn get_relates_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let relation = &input_row[argument_positions[0].as_usize()];
    let role = &input_row[argument_positions[1].as_usize()];
    if let Some(relates) = get_relates(context, relation, role)? {
        let metas = get_relates_meta_annotations(context, relates)?;
        put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)?;
    }
    Ok(())
}

pub(crate) fn get_sub_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let subtype = &input_row[argument_positions[0].as_usize()];
    let supertype = &input_row[argument_positions[1].as_usize()];
    let metas = get_subtype_all_meta(context, subtype, supertype)?;
    put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)
}

pub(crate) fn get_fun_doc(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing doc is equivalent to @doc("")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let function_name = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref();
    let function = context.function_manager().get_function_key(&**context.snapshot(), function_name).unwrap();
    let Some(function) = function else {
        return Err(Box::new(ConceptReadError::FunctionNotFound { function_name: function_name.to_owned() }));
    };
    row[return_position.as_usize()] = unwrap_doc(context.function_manager().get_function_annotation_by_category(
        &**context.snapshot(),
        function,
        &AnnotationCategory::Doc,
    )?);
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn get_fun_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let Some(return_position) = assignment_positions[0] else {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(()); // a missing metadata annotation for a key is treated as @meta("key", "")
    };
    let (mut row, multiplicity, provenance) = row_into_parts_widened(input_row, return_position);
    let key = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    let function_name = input_row[argument_positions[1].as_usize()].as_value().unwrap_string_ref();
    let function = context.function_manager().get_function_key(&**context.snapshot(), function_name).unwrap();
    let Some(function) = function else {
        return Err(Box::new(ConceptReadError::FunctionNotFound { function_name: function_name.to_owned() }));
    };
    row[return_position.as_usize()] = unwrap_meta_value(
        context.function_manager().get_function_annotation_by_category(&**context.snapshot(), function, category)?,
    );
    let output_row = MaybeOwnedRow::new_owned(row, multiplicity, provenance);
    output.append(|mut row| row.copy_from_row(output_row));
    Ok(())
}

pub(crate) fn get_fun_all_meta(
    assignment_positions: &[Option<VariablePosition>],
    argument_positions: &[VariablePosition],
    context: &ExecutionContext<impl ReadableSnapshot>,
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
) -> Result<(), Box<ConceptReadError>> {
    let key_return_position = assignment_positions[0];
    let value_return_position = assignment_positions[1];
    let function_name = input_row[argument_positions[0].as_usize()].as_value().unwrap_string_ref();
    let function = context.function_manager().get_function_key(&**context.snapshot(), function_name).unwrap();
    let Some(function) = function else {
        return Err(Box::new(ConceptReadError::FunctionNotFound { function_name: function_name.to_owned() }));
    };
    let metas = context
        .function_manager()
        .get_function_annotations(&**context.snapshot(), function)?
        .into_iter()
        .filter_map(|anno| match anno {
            FunctionAnnotation::Meta(annotation_meta) => Some(annotation_meta),
            _ => None,
        })
        .collect_vec();
    put_all_metas_into_batch(input_row, output, key_return_position, value_return_position, metas)
}

pub(crate) fn put_all_metas_into_batch(
    input_row: &MaybeOwnedRow<'_>,
    output: &mut FixedBatch,
    key_return_position: Option<VariablePosition>,
    value_return_position: Option<VariablePosition>,
    metas: Vec<AnnotationMeta>,
) -> Result<(), Box<ConceptReadError>> {
    if metas.is_empty() {
        return Ok(());
    } else if key_return_position.is_none() && value_return_position.is_none() {
        output.append(|mut row| row.copy_from_row(input_row.as_reference()));
        return Ok(());
    }

    let (mut row, multiplicity, provenance) =
        row_into_parts_widened(input_row, Ord::max(key_return_position, value_return_position).unwrap());
    for anno in metas {
        let (key, value) = meta_to_tuple(anno);
        if let Some(key_return_position) = key_return_position {
            row[key_return_position.as_usize()] = key;
        }
        if let Some(value_return_position) = value_return_position {
            row[value_return_position.as_usize()] = value;
        }
        let output_row = MaybeOwnedRow::new_owned(row.clone(), multiplicity, provenance);
        output.append(|mut row| row.copy_from_row(output_row));
    }
    Ok(())
}

pub(crate) fn row_into_parts_widened(
    input_row: &MaybeOwnedRow<'_>,
    index: VariablePosition,
) -> (Vec<VariableValue<'static>>, u64, Provenance) {
    let (mut row, multiplicity, provenance) = input_row.clone().into_owned_parts();
    if row.len() <= index.as_usize() {
        row.resize(index.as_usize() + 1, VariableValue::None);
    }
    (row, multiplicity, provenance)
}

pub(crate) fn get_owns(
    context: &ExecutionContext<impl ReadableSnapshot>,
    owner: &VariableValue<'_>,
    attribute: &VariableValue<'_>,
) -> Result<Option<Owns>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let owner = owner.as_type();
    let Type::Attribute(owned) = attribute.as_type() else { return Ok(None) };
    match owner {
        Type::Entity(ty) => ty.get_owns_attribute(snapshot, context.type_manager(), owned),
        Type::Relation(ty) => ty.get_owns_attribute(snapshot, context.type_manager(), owned),
        Type::Attribute(_) | Type::RoleType(_) => Ok(None),
    }
}

pub(crate) fn get_plays(
    context: &ExecutionContext<impl ReadableSnapshot>,
    player: &VariableValue<'_>,
    role: &VariableValue<'_>,
) -> Result<Option<Plays>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let player = player.as_type();
    let Type::RoleType(role) = role.as_type() else { return Ok(None) };
    match player {
        Type::Entity(ty) => ty.get_plays_role(snapshot, context.type_manager(), role),
        Type::Relation(ty) => ty.get_plays_role(snapshot, context.type_manager(), role),
        Type::Attribute(_) | Type::RoleType(_) => Ok(None),
    }
}

pub(crate) fn get_relates(
    context: &ExecutionContext<impl ReadableSnapshot>,
    relation: &VariableValue<'_>,
    role: &VariableValue<'_>,
) -> Result<Option<Relates>, Box<ConceptReadError>> {
    let Type::Relation(relation) = relation.as_type() else { return Ok(None) };
    let Type::RoleType(role) = role.as_type() else { return Ok(None) };
    relation.get_relates_role(&**context.snapshot(), context.type_manager(), role)
}

pub(crate) fn get_type_doc(
    context: &ExecutionContext<impl ReadableSnapshot>,
    ty: &VariableValue<'_>,
) -> Result<VariableValue<'static>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    match ty.as_type() {
        Type::Entity(ty) => Ok(unwrap_doc(context.type_manager().get_entity_type_annotation_declared_by_category(
            snapshot,
            ty,
            &AnnotationCategory::Doc,
        )?)),
        Type::Relation(ty) => {
            Ok(unwrap_doc(context.type_manager().get_relation_type_annotation_declared_by_category(
                snapshot,
                ty,
                &AnnotationCategory::Doc,
            )?))
        }
        Type::Attribute(ty) => {
            Ok(unwrap_doc(context.type_manager().get_attribute_type_annotation_declared_by_category(
                snapshot,
                ty,
                &AnnotationCategory::Doc,
            )?))
        }
        Type::RoleType(ty) => {
            let relates = ty.get_relates_explicit(snapshot, context.type_manager())?;
            Ok(unwrap_doc(context.type_manager().get_relates_annotation_declared_by_category(
                snapshot,
                relates,
                &AnnotationCategory::Doc,
            )?))
        }
    }
}

pub(crate) fn unwrap_doc(doc: Option<impl Into<Annotation>>) -> VariableValue<'static> {
    match doc.map(Into::into) {
        Some(Annotation::Doc(doc)) => VariableValue::Value(Value::String(Cow::Owned(doc.doc))),
        None => VariableValue::Value(Value::String(Cow::default())),
        Some(anno) => unreachable!("Internal error: Expected AnnotationDoc, got {anno:?}"),
    }
}

pub(crate) fn get_type_meta(
    context: &ExecutionContext<impl ReadableSnapshot>,
    key: &VariableValue<'_>,
    ty: &VariableValue<'_>,
) -> Result<VariableValue<'static>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let key = key.as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    match ty.as_type() {
        Type::Entity(ty) => Ok(unwrap_meta_value(
            context.type_manager().get_entity_type_annotation_declared_by_category(snapshot, ty, category)?,
        )),
        Type::Relation(ty) => Ok(unwrap_meta_value(
            context.type_manager().get_relation_type_annotation_declared_by_category(snapshot, ty, category)?,
        )),
        Type::Attribute(ty) => Ok(unwrap_meta_value(
            context.type_manager().get_attribute_type_annotation_declared_by_category(snapshot, ty, category)?,
        )),
        Type::RoleType(ty) => {
            let relates = ty.get_relates_explicit(snapshot, context.type_manager())?;
            Ok(unwrap_meta_value(
                context.type_manager().get_relates_annotation_declared_by_category(snapshot, relates, category)?,
            ))
        }
    }
}

pub(crate) fn unwrap_meta_value(meta: Option<impl Into<Annotation>>) -> VariableValue<'static> {
    match meta.map(Into::into) {
        Some(Annotation::Meta(meta)) => VariableValue::Value(Value::String(Cow::Owned(meta.value))),
        None => VariableValue::Value(Value::String(Cow::default())),
        Some(anno) => unreachable!("Internal error: Expected AnnotationMeta, got {anno:?}"),
    }
}

pub(crate) fn get_type_all_meta(
    context: &ExecutionContext<impl ReadableSnapshot>,
    ty: &VariableValue<'_>,
) -> Result<Vec<AnnotationMeta>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    match ty.as_type() {
        Type::Entity(ty) => Ok(context
            .type_manager()
            .get_entity_type_annotations_declared(snapshot, ty)?
            .iter()
            .filter_map(|anno| match anno {
                EntityTypeAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                _ => None,
            })
            .collect()),
        Type::Relation(ty) => Ok(context
            .type_manager()
            .get_relation_type_annotations_declared(snapshot, ty)?
            .iter()
            .filter_map(|anno| match anno {
                RelationTypeAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                _ => None,
            })
            .collect()),
        Type::Attribute(ty) => Ok(context
            .type_manager()
            .get_attribute_type_annotations_declared(snapshot, ty)?
            .iter()
            .filter_map(|anno| match anno {
                AttributeTypeAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                _ => None,
            })
            .collect()),
        Type::RoleType(ty) => {
            let relates = ty.get_relates_explicit(snapshot, context.type_manager())?;
            get_relates_meta_annotations(context, relates)
        }
    }
}

pub(crate) fn get_plays_meta_annotations(
    context: &ExecutionContext<impl ReadableSnapshot>,
    plays: Plays,
) -> Result<Vec<AnnotationMeta>, Box<ConceptReadError>> {
    Ok(context
        .type_manager()
        .get_plays_annotations_declared(&**context.snapshot(), plays)?
        .iter()
        .filter_map(|anno| match anno {
            PlaysAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
            _ => None,
        })
        .collect())
}

pub(crate) fn get_owns_meta_annotations(
    context: &ExecutionContext<impl ReadableSnapshot>,
    owns: Owns,
) -> Result<Vec<AnnotationMeta>, Box<ConceptReadError>> {
    Ok(context
        .type_manager()
        .get_owns_annotations_declared(&**context.snapshot(), owns)?
        .iter()
        .filter_map(|anno| match anno {
            OwnsAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
            _ => None,
        })
        .collect())
}

pub(crate) fn get_relates_meta_annotations(
    context: &ExecutionContext<impl ReadableSnapshot>,
    relates: Relates,
) -> Result<Vec<AnnotationMeta>, Box<ConceptReadError>> {
    Ok(context
        .type_manager()
        .get_relates_annotations_declared(&**context.snapshot(), relates)?
        .iter()
        .filter_map(|anno| match anno {
            RelatesAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
            _ => None,
        })
        .collect())
}

pub(crate) fn meta_to_tuple(meta: AnnotationMeta) -> (VariableValue<'static>, VariableValue<'static>) {
    (
        VariableValue::Value(Value::String(Cow::Owned(meta.key))),
        VariableValue::Value(Value::String(Cow::Owned(meta.value))),
    )
}

pub(crate) fn get_subtype_doc(
    context: &ExecutionContext<impl ReadableSnapshot>,
    subtype: &VariableValue<'_>,
    supertype: &VariableValue<'_>,
) -> Result<Option<VariableValue<'static>>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let type_manager = context.type_manager();
    match (subtype.as_type(), supertype.as_type()) {
        (Type::Entity(subtype), Type::Entity(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_doc(type_manager.get_sub_entity_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                &AnnotationCategory::Doc,
            )?)))
        }
        (Type::Relation(subtype), Type::Relation(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_doc(type_manager.get_sub_relation_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                &AnnotationCategory::Doc,
            )?)))
        }
        (Type::Attribute(subtype), Type::Attribute(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_doc(type_manager.get_sub_attribute_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                &AnnotationCategory::Doc,
            )?)))
        }
        (Type::RoleType(subtype), Type::RoleType(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(VariableValue::Value(Value::String(Cow::Borrowed(""))))) // no annotations can exist on relates r2 as r1;
        }
        (_, _) => Ok(None),
    }
}

pub(crate) fn get_subtype_meta(
    context: &ExecutionContext<impl ReadableSnapshot>,
    key: &VariableValue<'_>,
    subtype: &VariableValue<'_>,
    supertype: &VariableValue<'_>,
) -> Result<Option<VariableValue<'static>>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let type_manager = context.type_manager();
    let key = key.as_value().unwrap_string_ref().to_owned();
    let category = &AnnotationCategory::Meta(key);
    match (subtype.as_type(), supertype.as_type()) {
        (Type::Entity(subtype), Type::Entity(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_meta_value(type_manager.get_sub_entity_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                category,
            )?)))
        }
        (Type::Relation(subtype), Type::Relation(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_meta_value(type_manager.get_sub_relation_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                category,
            )?)))
        }
        (Type::Attribute(subtype), Type::Attribute(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(unwrap_meta_value(type_manager.get_sub_attribute_type_annotations_declared_by_category(
                snapshot,
                Sub::new(subtype, supertype),
                category,
            )?)))
        }
        (Type::RoleType(subtype), Type::RoleType(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(None);
            }
            Ok(Some(VariableValue::Value(Value::String(Cow::Borrowed(""))))) // no annotations can exist on relates r2 as r1;
        }
        (_, _) => Ok(None),
    }
}

pub(crate) fn get_subtype_all_meta(
    context: &ExecutionContext<impl ReadableSnapshot>,
    subtype: &VariableValue<'_>,
    supertype: &VariableValue<'_>,
) -> Result<Vec<AnnotationMeta>, Box<ConceptReadError>> {
    let snapshot = &**context.snapshot();
    let type_manager = context.type_manager();
    match (subtype.as_type(), supertype.as_type()) {
        (Type::Entity(subtype), Type::Entity(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(Vec::new());
            }
            Ok(type_manager
                .get_sub_entity_type_annotations_declared(snapshot, Sub::new(subtype, supertype))?
                .iter()
                .filter_map(|anno| match anno {
                    SubAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                    _ => None,
                })
                .collect())
        }
        (Type::Relation(subtype), Type::Relation(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(Vec::new());
            }
            Ok(type_manager
                .get_sub_relation_type_annotations_declared(snapshot, Sub::new(subtype, supertype))?
                .iter()
                .filter_map(|anno| match anno {
                    SubAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                    _ => None,
                })
                .collect())
        }
        (Type::Attribute(subtype), Type::Attribute(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(Vec::new());
            }
            Ok(type_manager
                .get_sub_attribute_type_annotations_declared(snapshot, Sub::new(subtype, supertype))?
                .iter()
                .filter_map(|anno| match anno {
                    SubAnnotation::Meta(annotation_meta) => Some(annotation_meta.clone()),
                    _ => None,
                })
                .collect())
        }
        (Type::RoleType(subtype), Type::RoleType(supertype)) => {
            if subtype.get_supertype(snapshot, type_manager)? != Some(supertype) {
                return Ok(Vec::new());
            }
            Ok(Vec::new()) // no annotations can exist on relates r2 as r1;
        }
        (_, _) => Ok(Vec::new()),
    }
}

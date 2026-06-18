/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{Thing, variable_value::VariableValue};
use compiler::{VariablePosition, annotation::function::FunctionParameterAnnotation};
use concept::error::ConceptDecodeError;
use encoding::value::{ValueEncodable, value::Value, value_type::ValueType};
use error::typedb_error;
use executor::batch::Batch;
use ir::LiteralParseError;

pub trait GivenRows: Sized {
    type Item;
    type Row;
    fn variables(&self) -> &[String];
    fn decode(
        item: Self::Item,
        expected_type: &FunctionParameterAnnotation,
    ) -> Result<GivenRowEntry, GivenRowDecodeError>;

    fn row_count(&self) -> usize;
    fn rows(self) -> impl Iterator<Item = Self::Row>;
    fn iter_row(row: Self::Row) -> impl Iterator<Item = Self::Item>;

    fn into_batch_mapped(
        self,
        declared_variable_positions: &HashMap<&str, VariablePosition>,
        expected_types: &[FunctionParameterAnnotation],
    ) -> Result<Batch, GivenRowDecodeError> {
        let mapping =
            self.variables()
                .iter()
                .map(|name| {
                    declared_variable_positions.get(&name.as_str()).copied().ok_or_else(|| {
                        GivenRowDecodeError::GivenRowsVariableWasNotDeclared { variable: name.to_owned() }
                    })
                })
                .collect::<Result<Vec<VariablePosition>, GivenRowDecodeError>>()?;

        let width = declared_variable_positions.len() as u32;
        let mut batch = Batch::new(width, self.row_count());
        self.rows().into_iter().try_for_each(|row| {
            batch.append(|mut write_to| {
                Self::iter_row(row).enumerate().try_for_each(|(column, entry)| {
                    let target_index = mapping[column];
                    let expected_type = &expected_types[target_index.as_usize()];
                    let value = match Self::decode(entry, expected_type)? {
                        GivenRowEntry::None => VariableValue::None,
                        GivenRowEntry::Thing(thing) => VariableValue::Thing(thing),
                        GivenRowEntry::Value(value) => VariableValue::Value(value),
                    };
                    write_to.set(target_index, value);
                    Ok(())
                })
            })
        })?;
        Ok(batch)
    }
}

#[derive(Debug, Clone)]
pub enum GivenRowEntry {
    None,
    Thing(Thing),
    Value(Value<'static>),
}

impl GivenRowEntry {
    pub fn try_cast_value_to(
        value: Value<'static>,
        expected_type: &FunctionParameterAnnotation,
    ) -> Result<Self, GivenRowDecodeError> {
        let FunctionParameterAnnotation::Value(expected_type) = expected_type else {
            return Err(GivenRowDecodeError::ExpectedInstanceReceivedValue {});
        };
        let make_err = {
            let actual_type = value.value_type().to_string();
            let expected_type = expected_type.clone();
            |value: String| GivenRowDecodeError::ValueTypeMismatch { expected_type, actual_type, value }
        };

        if value.value_type().is_trivially_castable_to(expected_type.category()) {
            let cast_result = value.cast(expected_type.category());
            debug_assert!(cast_result.is_some());
            let cast_value = cast_result.ok_or_else(|| make_err("<unreachable>".to_owned()))?;
            Ok(GivenRowEntry::Value(cast_value))
        } else {
            Err(make_err(value.to_string()))
        }
    }
}

typedb_error! {
    pub GivenRowDecodeError(component = "Decoding given rows", prefix = "GVN") {
        ConceptDecode(1, "An error occurred while decoding the provided concept.", typedb_source: Box<ConceptDecodeError>),
        InvalidIIDFormatForGivenEntry(2, "The provided iid string '{iid}' was invalid.", iid: String),
        ParsingValueFailedForGivenEntry(3, "An error occured while parsing the provided value '{value}'.", value: String, typedb_source: typeql::Error),
        TranslatingValueFailedForGivenEntry(4, "An error occured while translating the provided value '{value}'.", value: String, typedb_source: LiteralParseError),
        GivenRowsVariableWasNotDeclared(5, "The variable '{variable}' was not declared in the query.", variable: String),
        ExpectedInstanceReceivedValue(6, "A value was provided where a concept instance was expected."),
        ValueTypeMismatch(7, "The provided value '{value}' has the JSON type '{actual_type}' and could not be decoded as the value type '{expected_type}'.", expected_type: ValueType, actual_type: String, value: String),
    }
}

#[derive(Debug)]
pub struct GivenRowsSimple {
    pub variables: Vec<String>,
    pub rows: Vec<Vec<GivenRowEntry>>,
}

impl GivenRows for GivenRowsSimple {
    type Item = GivenRowEntry;
    type Row = Vec<GivenRowEntry>;
    fn variables(&self) -> &[String] {
        self.variables.as_slice()
    }

    fn decode(
        item: Self::Item,
        expected_type: &FunctionParameterAnnotation,
    ) -> Result<GivenRowEntry, GivenRowDecodeError> {
        match item {
            GivenRowEntry::Value(value) => GivenRowEntry::try_cast_value_to(value, expected_type),
            other => Ok(other),
        }
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn rows(self) -> impl Iterator<Item = Self::Row> {
        self.rows.into_iter()
    }

    fn iter_row(row: Self::Row) -> impl Iterator<Item = Self::Item> {
        row.into_iter()
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{Thing, variable_value::VariableValue};
use compiler::VariablePosition;
use concept::error::ConceptDecodeError;
use encoding::value::value::Value;
use error::typedb_error;
use executor::batch::Batch;
use ir::LiteralParseError;

pub trait GivenRows {
    fn into_batch_mapped(
        self,
        declared_variable_positions: &HashMap<&str, VariablePosition>,
    ) -> Result<Batch, GivenRowDecodeError>;
}

pub trait GivenRowsDecoder<T> {
    fn decode(what: T) -> Result<GivenRowEntry, GivenRowDecodeError>;
}

#[derive(Debug, Clone)]
pub enum GivenRowEntry {
    None,
    Thing(Thing),
    Value(Value<'static>),
}

pub fn into_batch_mapped<T, Row, Decoder>(
    declared_variable_positions: &HashMap<&str, VariablePosition>,
    variables: Vec<String>,
    row_count: usize,
    rows: impl Iterator<Item = Row>,
) -> Result<Batch, GivenRowDecodeError>
where
    Row: IntoIterator<Item = T>,
    Decoder: GivenRowsDecoder<T>,
{
    let mapping = variables
        .iter()
        .map(|name| {
            declared_variable_positions
                .get(&name.as_str())
                .copied()
                .ok_or_else(|| GivenRowDecodeError::GivenRowsVariableWasNotDeclared { variable: name.to_owned() })
        })
        .collect::<Result<Vec<VariablePosition>, GivenRowDecodeError>>()?;

    let width = declared_variable_positions.len() as u32;
    let mut batch = Batch::new(width, row_count);
    rows.into_iter().try_for_each(|row| {
        batch.append(|mut write_to| {
            row.into_iter().enumerate().try_for_each(|(column, entry)| {
                let value = match Decoder::decode(entry)? {
                    GivenRowEntry::None => VariableValue::None,
                    GivenRowEntry::Thing(thing) => VariableValue::Thing(thing),
                    GivenRowEntry::Value(value) => VariableValue::Value(value),
                };
                write_to.set(mapping[column], value);
                Ok(())
            })
        })
    })?;
    Ok(batch)
}

typedb_error! {
    pub GivenRowDecodeError(component = "Decoding given rows", prefix = "GVN") {
        ConceptDecode(1, "An error occurred while decoding the provided concept.", typedb_source: Box<ConceptDecodeError>),
        InvalidIIDFormatForGivenEntry(2, "The provided iid string '{iid}' was invalid.", iid: String),
        ParsingValueFailedForGivenEntry(3, "An error occured while parsing the provided value '{value}'.", value: String, typedb_source: typeql::Error),
        TranslatingValueFailedForGivenEntry(4, "An error occured while translating the provided value '{value}'.", value: String, typedb_source: LiteralParseError),
        GivenRowsVariableWasNotDeclared(5, "The variable {variable} was not declared in the query.", variable: String),
    }
}

#[derive(Debug)]
pub struct GivenRowsSimple {
    pub variables: Vec<String>,
    pub rows: Vec<Vec<GivenRowEntry>>,
}

impl GivenRows for GivenRowsSimple {
    fn into_batch_mapped(
        self,
        declared_variable_positions: &HashMap<&str, VariablePosition>,
    ) -> Result<Batch, GivenRowDecodeError> {
        self::into_batch_mapped::<GivenRowEntry, Vec<GivenRowEntry>, GivenRowsSimpleDecoder>(
            declared_variable_positions,
            self.variables,
            self.rows.len(),
            self.rows.into_iter(),
        )
    }
}

struct GivenRowsSimpleDecoder {}
impl GivenRowsDecoder<GivenRowEntry> for GivenRowsSimpleDecoder {
    fn decode(what: GivenRowEntry) -> Result<GivenRowEntry, GivenRowDecodeError> {
        Ok(what)
    }
}

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

pub trait GivenRows: Sized {
    type Item;
    type Row;
    fn variables(&self) -> &[String];
    fn decode(item: Self::Item) -> Result<GivenRowEntry, GivenRowDecodeError>;

    fn row_count(&self) -> usize;
    fn rows(self) -> impl Iterator<Item = Self::Row>;
    fn iter_row(row: Self::Row) -> impl Iterator<Item = Self::Item>;

    fn into_batch_mapped(
        self,
        declared_variable_positions: &HashMap<&str, VariablePosition>,
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
                    let value = match Self::decode(entry)? {
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
}

#[derive(Debug, Clone)]
pub enum GivenRowEntry {
    None,
    Thing(Thing),
    Value(Value<'static>),
}

typedb_error! {
    pub GivenRowDecodeError(component = "Decoding given rows", prefix = "GVN") {
        ConceptDecode(1, "An error occurred while decoding the provided concept.", typedb_source: Box<ConceptDecodeError>),
        InvalidIIDFormatForGivenEntry(2, "The provided iid string '{iid}' was invalid.", iid: String),
        ParsingValueFailedForGivenEntry(3, "An error occured while parsing the provided value '{value}'.", value: String, typedb_source: typeql::Error),
        TranslatingValueFailedForGivenEntry(4, "An error occured while translating the provided value '{value}'.", value: String, typedb_source: LiteralParseError),
        GivenRowsVariableWasNotDeclared(5, "The variable '{variable}' was not declared in the query.", variable: String),
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

    fn decode(item: Self::Item) -> Result<GivenRowEntry, GivenRowDecodeError> {
        Ok(item)
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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::{variable::Variable, variable_value::VariableValue};
use compiler::insert::{ThingSource, TypeSource, ValueSource, VariableSource};
use encoding::value::value::Value;

use crate::{batch::Row, VariablePosition};

fn get_type<'a>(input: &'a Row<'a>, source: &'a TypeSource) -> &'a answer::Type {
    match source {
        TypeSource::InputVariable(position) => input.get(position.clone()).as_type(),
        TypeSource::TypeConstant(type_) => type_,
    }
}

fn get_thing<'a>(input: &'a Row<'a>, source: &'a ThingSource) -> &'a answer::Thing<'static> {
    let ThingSource(position) = source;
    input.get(position.clone()).as_thing()
}

fn get_value<'a>(input: &'a Row<'a>, source: &'a ValueSource) -> &'a Value<'static> {
    match source {
        ValueSource::InputVariable(position) => input.get(position.clone()).as_value(),
        ValueSource::ValueConstant(constant) => constant,
    }
}

pub fn populate_output_row<'input>(
    output_row_plan: &[(Variable, VariableSource)],
    input: &Row<'input>,
    freshly_inserted: &[answer::Thing<'static>],
    output: &mut [VariableValue<'static>],
) {
    for (i, (_, source)) in output_row_plan.iter().enumerate() {
        let value = match source {
            VariableSource::InputVariable(s) => input.get(s.clone()).clone(),
            VariableSource::InsertedThing(s) => VariableValue::Thing(freshly_inserted.get(*s).unwrap().clone()),
        };
        output[i] = value;
    }
}

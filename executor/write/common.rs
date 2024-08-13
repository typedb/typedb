/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use compiler::write::{ThingSource, TypeSource, ValueSource, VariableSource};
use encoding::value::value::Value;

use crate::{batch::Row, VariablePosition};

fn get_type<'a>(input: &'a Row<'a>, source: &'a TypeSource) -> &'a answer::Type {
    match source {
        TypeSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_type(),
        TypeSource::TypeConstant(type_) => type_,
    }
}

fn get_thing<'a>(
    input: &'a Row<'a>,
    freshly_inserted: &'a [answer::Thing<'static>],
    source: &'a ThingSource,
) -> &'a answer::Thing<'static> {
    match source {
        ThingSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_thing(),
        ThingSource::InsertedThing(offset) => freshly_inserted.get(*offset).unwrap(),
    }
}

fn get_value<'a>(input: &'a Row<'a>, source: &'a ValueSource) -> &'a Value<'static> {
    match source {
        ValueSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_value(),
        ValueSource::ValueConstant(constant) => constant,
    }
}

pub fn populate_output_row<'input, 'output>(
    output_row_plan: &[VariableSource],
    input: &Row<'input>,
    freshly_inserted: &[answer::Thing<'static>],
    output: &mut Row<'output>,
) {
    for (i, source) in output_row_plan.iter().enumerate() {
        let value = match source {
            VariableSource::InputVariable(s) => input.get(VariablePosition::new(*s)).clone(),
            VariableSource::InsertedThing(s) => VariableValue::Thing(freshly_inserted.get(*s).unwrap().clone()),
        };
        output.set(VariablePosition::new(i as u32), value)
    }
    output.set_multiplicity(1);
}

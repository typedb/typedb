/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::HashMap;
use crate::graph::definition::r#struct::StructFieldNumber;
use crate::value::ValueEncodable;

#[derive(Debug)]
pub struct StructValue {
    // a map allows empty fields to not be recorded at all
    fields: HashMap<StructFieldNumber, Box<dyn ValueEncodable>>
}

// TODO: implement serialise/deserialise for the StructValue
//       since JSON serialisation seems to be able to handle recursive nesting, it should be able to handle that

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use encoding::graph::definition::definition_key::DefinitionKey;
use resource::constants::encoding::StructFieldIDUInt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Visitor};

use encoding::error::EncodingError;
use encoding::graph::definition::r#struct::{StructDefinition, StructDefinitionField};

use encoding::value::ValueEncodable;
use iterator::Collector;

use crate::thing::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct StructValue<'a> {
    definition: DefinitionKey<'static>,

    // a map allows empty fields to not be recorded at all
    fields: HashMap<StructFieldIDUInt, Value<'a>>,
}

impl<'a> StructValue<'a> {
    // TODO: Return vec<ValueTypeMismatch>
    pub fn try_translate_fields(struct_definition: StructDefinition, value: HashMap<String, Value<'a>>) -> Result<HashMap<StructFieldIDUInt, Value<'a>>, Vec<EncodingError>> {
        let mut fields : HashMap<StructFieldIDUInt, Value<'a>> = HashMap::new();
        let mut errors: Vec<EncodingError> = Vec::new();
        for (field_name, field_id) in struct_definition.field_names {
            let field_definition: &StructDefinitionField = &struct_definition.fields.get(field_id as usize).unwrap();
            if let Some(value) = value.get(&field_name) {
                if field_definition.value_type == value.value_type()  {
                    fields.insert(field_id, value.clone());
                } else {
                    errors.add(EncodingError::StructFieldValueTypeMismatch {
                        field_name, expected: field_definition.value_type.clone(),
                    })
                }
            } else if !field_definition.optional {
                errors.add(EncodingError::StructMissingRequiredField { field_name })
            }
        }

        if errors.is_empty() {
            Ok(fields)
        } else {
            Err(errors)
        }
    }

    pub fn definition_key(&self) -> DefinitionKey<'_> {
        self.definition.as_reference()
    }

}

// TODO: implement serialise/deserialise for the StructValue
//       since JSON serialisation seems to be able to handle recursive nesting, it should be able to handle that
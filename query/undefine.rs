/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{Annotation, AnnotationError},
        attribute_type::AttributeType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager,
        Ordering,
    },
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value_type::ValueType},
};
use ir::LiteralParseError;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{
    query::schema::Undefine,
    schema::{
        definable::{
            struct_::Field,
            type_::{
                capability::{Owns as TypeQLOwns, Plays as TypeQLPlays, Relates as TypeQLRelates},
                Capability, CapabilityBase,
            },
            Type,
        },
        undefinable::{Struct, Undefinable},
    },
    type_::Optional,
    ScopedLabel, TypeRef, TypeRefAny,
};

use crate::definition_resolution::{filter_variants, resolve_value_type, try_unwrap, SymbolResolutionError};

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    todo!()
}

#[derive(Debug)]
pub enum UndefineError {
    Unimplemented,

}

impl fmt::Display for UndefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for UndefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}

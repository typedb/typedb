/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptReadError;
use encoding::{graph::type_::Kind, value::label::Label};
use typeql::schema::definable::type_::Capability;

mod define;
mod error;
mod match_;
mod pipeline;
pub mod query_manager;
mod util;

#[derive(Debug)]
pub enum SymbolResolutionError {
    TypeNotFound { label: Label<'static> },
    KindMismatch { label: Label<'static>, expected: Kind, actual: Kind, capability: Capability },
    ValueTypeNotFound { name: String },
    IllegalValueTypeName { scope: String, name: String },
    UnexpectedConceptRead { source: ConceptReadError },
}

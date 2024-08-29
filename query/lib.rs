/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptReadError;
use encoding::{graph::type_::Kind, value::label::Label};
use typeql::schema::definable::type_::Capability;

mod annotation;
mod compilation;
mod define;
mod definition_resolution;
mod definition_status;
pub mod error;
pub mod query_manager;
mod redefine;
mod translation;
mod undefine;

#[derive(Debug)]
pub enum SymbolResolutionError {
    TypeNotFound { label: Label<'static> },
    KindMismatch { label: Label<'static>, expected: Kind, actual: Kind, capability: Capability },
    ValueTypeNotFound { name: String },
    IllegalValueTypeName { scope: String, name: String },
    UnexpectedConceptRead { source: ConceptReadError },
}

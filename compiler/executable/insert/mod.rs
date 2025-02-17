/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
};

use answer::{variable::Variable, Type};
use encoding::graph::type_::Kind;
use error::typedb_error;
use ir::{
    pattern::{constraint, constraint::Comparator, ParameterID, Vertex},
    pipeline::VariableRegistry,
};
use itertools::Itertools;
use typeql::common::Span;

use crate::{annotation::type_annotations::TypeAnnotations, executable::WriteCompilationError, VariablePosition};

pub mod executable;
pub mod instructions;
pub mod type_check;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum VariableSource {
    InputVariable(VariablePosition), // TODO: This needs to be renamed
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum TypeSource {
    InputVariable(VariablePosition),
    Constant(answer::Type),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ValueSource {
    Variable(VariablePosition),
    Parameter(ParameterID),
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct ThingPosition(pub VariablePosition);

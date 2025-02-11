/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU64, Ordering};

use error::typedb_error;
use ir::pattern::constraint::Comparator;
use typeql::common::Span;

use crate::executable::{fetch::executable::FetchCompilationError, match_::planner::MatchCompilationError};

pub mod delete;
pub mod fetch;
pub mod function;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod pipeline;
pub mod reduce;
pub mod update;

static EXECUTABLE_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_executable_id() -> u64 {
    EXECUTABLE_ID.fetch_add(1, Ordering::Relaxed)
}

typedb_error! {
    pub ExecutableCompilationError(component = "Executable compiler", prefix = "ECP") {
        InsertExecutableCompilation(1, "Error compiling insert stage into executable.", typedb_source: Box<WriteCompilationError>),
        UpdateExecutableCompilation(2, "Error compiling update clause into executable.", typedb_source: Box<WriteCompilationError>),
        DeleteExecutableCompilation(3, "Error compiling delete stage into executable.", typedb_source: Box<WriteCompilationError>),
        FetchCompilation(4, "Error compiling fetch stage into executable.", typedb_source: FetchCompilationError),
        MatchCompilation(5, "Error compiling match stage into executable.", typedb_source: MatchCompilationError),
    }
}

typedb_error! {
    pub WriteCompilationError(component = "Write compiler", prefix = "WCP") {
        InsertIsaStatementForInputVariable(
            1,
            "Illegal 'isa' provided for variable '{variable}' that is input from a previous stage - 'isa's should only be used to create new instances in insert stages.",
            variable: String,
            source_span: Option<Span>,
        ),
        MultipleInsertsForSameVariable(
            2,
            "Found multiple insert statements for the variable '{variable}'.",
            variable: String,
        ),
        InsertVariableAmbiguousAttributeOrObject(
            3,
            "Insert variable '{variable}' is ambiguously an attribute or an object (entity/relation).",
            variable: String,
            source_span: Option<Span>,
        ),
        InsertVariableUnknownType(
            4,
            "Could not determine the type of the insert variable '{variable}'.",
            variable: String,
            source_span: Option<Span>,
        ),
        InsertAttributeMissingValue(
            5,
            "Could not determine the value of the insert attribute '{variable}'.",
            variable: String,
            source_span: Option<Span>,
        ),
        InsertIllegalPredicate(
            6,
            "Illegal predicate in insert for variable '{variable}' with comparator '{comparator}'.",
            variable: String,
            comparator: Comparator,
            source_span: Option<Span>,
        ),
        MissingExpectedInput(
            7,
            "Missing expected input variable in compilation data '{variable}'.",
            variable: String,
            source_span: Option<Span>,
        ),
        AmbiguousRoleType(
            8,
            "Could not uniquely resolve the role type for variable '{variable}'. Possible role types are: {role_types}.",
            variable: String,
            role_types: String,
            source_span: Option<Span>,
        ),
        InsertLinksAmbiguousRoleType(
            9,
            "Links insert for player '{player_variable}' requires unambiguous role type, but inferred: {role_types}.",
            player_variable: String,
            role_types: String,
            source_span: Option<Span>,
        ),
        DeleteIllegalRoleVariable(
            10,
            "Illegal delete for variable '{variable}', which represents role types.",
            variable: String,
            source_span: Option<Span>,
        ),
        InsertIllegalRole(
            11,
            "Illegal role type insert for variable '{variable}'.",
            variable: String,
            source_span: Option<Span>,
        ),
        DeletedThingWasNotInInput(
            12,
            "Deleted variable '{variable}' is not available as input from previous stages.",
            variable: String,
            source_span: Option<Span>,
        ),
        UnsupportedCompoundExpressions(
            13,
            "Compound expressions are not supported in these statements yet.",
            source_span: Option<Span>,
        ),
    }
}

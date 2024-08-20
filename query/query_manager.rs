/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use function::function::Function;
use storage::snapshot::WritableSnapshot;
use typeql::{query::SchemaQuery, Query};

use crate::{define, error::QueryError};

pub struct QueryManager {}

impl QueryManager {
    // TODO: clean up if QueryManager remains stateless
    pub fn new() -> QueryManager {
        QueryManager {}
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        query: SchemaQuery,
    ) -> Result<(), QueryError> {
        match query {
            SchemaQuery::Define(define) => define::execute(snapshot, type_manager, thing_manager, define)
                .map_err(|err| QueryError::Define { source: err }),
            SchemaQuery::Redefine(redefine) => {
                todo!()
            }
            SchemaQuery::Undefine(undefine) => {
                todo!()
            }
        }
    }

    // TODO: take in parsed TypeQL clause
    fn create_executor(&self, clause: &str) {
        // match clause
    }

    fn create_match_executor(&self, query_functions: Vec<Function<usize>>) {
        // let conjunction = Conjunction::new();
        // ... build conjunction...
    }
}

enum Stage {
    Match,
    Insert,
    Delete,
    Put,
    Fetch,
    Assert,
    Select,
    Sort,
    Offset,
    Limit,
}

trait PipelineStage {}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}

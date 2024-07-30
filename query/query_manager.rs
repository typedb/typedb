/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::type_manager::TypeManager;
use function::function::Function;
use storage::snapshot::WritableSnapshot;
use typeql::{query::SchemaQuery, Query};

use crate::{define, error::QueryError};

pub struct QueryManager {}

impl QueryManager {
    pub fn new() -> QueryManager {
        QueryManager {}
    }

    pub fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        query: &str,
    ) -> Result<(), QueryError> {
        let parsed = typeql::parse_query(query)
            .map_err(|err| QueryError::ParseError { typeql_query: query.to_string(), source: err })?;

        match parsed {
            Query::Schema(query) => match query {
                SchemaQuery::Define(define) => {
                    define::execute(snapshot, type_manager, define).map_err(|err| QueryError::Define { source: err })?
                }
                SchemaQuery::Redefine(redefine) => {
                    todo!()
                }
                SchemaQuery::Undefine(undefine) => {
                    todo!()
                }
            },
            Query::Pipeline(pipeline) => {}
        }

        // 1. parse query into list of TypeQL clauses
        // 2. expand implicit clauses, eg. fetch clause; -> filter clause; fetch clause;
        // 3. parse query-bound functions
        // 4. generate list of executors
        // 5. Execute each executor

        Ok(())
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

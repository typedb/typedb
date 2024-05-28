/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

struct QueryManager {

}

impl QueryManager {

    fn execute(&self, query: &str) {
        // 1. parse query into list of TypeQL clauses
        // 2. expand implicit clauses, eg. fetch clause; -> filter clause; fetch clause;
        // 3. generate list of executors
    }
}

enum Executor {
    Match,
    Insert,
    Delete,
    Put,
    Fetch,
    Assert,
    Filter,
    Sort,
    Offset,
    Limit
}

trait PipelineStage {

}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}

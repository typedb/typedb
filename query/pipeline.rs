/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

struct Pipeline {
    stages: Vec<Stage>
}


enum Stage {
    Match(MatchClause),
    Insert(InsertClause),
    Delete(DeleteClause),
    Put(PutClause),
    Update(UpdateClause),
    Fetch(FetchClause),
    OperatorSelect(SelectOperator),
    OperatorDistinct(DistinctOperator),
}

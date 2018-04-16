/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.InsertQueryAdmin;

import javax.annotation.CheckReturnValue;
import java.util.List;

/**
 * A query for inserting data.
 * <p>
 * A {@link InsertQuery} can be built from a {@link QueryBuilder} or a {@link Match}.
 * <p>
 * When built from a {@code QueryBuilder}, the insert query will execute once, inserting all the variables provided.
 * <p>
 * When built from a {@link Match}, the {@link InsertQuery} will execute for each result of the {@link Match},
 * where variable names in the {@link InsertQuery} are bound to the concept in the result of the {@link Match}.
 *
 * @author Felix Chapman
 */
public interface InsertQuery extends Query<List<Answer>>, Streamable<Answer> {

    /**
     * @param tx the graph to execute the query on
     * @return a new InsertQuery with the graph set
     */
    @Override
    InsertQuery withTx(GraknTx tx);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    InsertQueryAdmin admin();

}

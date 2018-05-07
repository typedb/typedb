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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn;

import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.UndefineQuery;

import java.util.stream.Stream;

/**
 * Interface for executing queries and getting a result. Examples of possible implementations are: running the query
 * against a tinkerpop graph, or sending the query to some server to execute via gRPC or a REST API.
 *
 * <p>
 * This class allows us to decouple query representation (in {@link ai.grakn.graql.Query}) from query execution
 * (here in {@link QueryExecutor}).
 * </p>
 *
 * @author Felix Chapman
 */
public interface QueryExecutor {

    Stream<ai.grakn.graql.admin.Answer> run(GetQuery query);

    Stream<ai.grakn.graql.admin.Answer> run(InsertQuery query);

    void run(DeleteQuery query);

    ai.grakn.graql.admin.Answer run(DefineQuery query);

    void run(UndefineQuery query);

    <T> T run(AggregateQuery<T> query);

    ComputeJob<ComputeQuery.Answer> run(ComputeQuery query);
}

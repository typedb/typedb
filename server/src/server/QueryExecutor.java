/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server;

import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.UndefineQuery;

import java.util.stream.Stream;

/**
 * Interface for executing queries and getting a result. Examples of possible implementations are: running the query
 * against a tinkerpop graph, or sending the query to some server to execute via gRPC or a REST API.
 *
 * This class allows us to decouple query representation from query execution
 * (here in {@link QueryExecutor}).
 */
public interface QueryExecutor {

    Stream<ConceptMap> run(DefineQuery query);

    Stream<ConceptMap> run(UndefineQuery query);

    Stream<ConceptMap> run(GetQuery query);

    Stream<ConceptMap> run(InsertQuery query);

    Stream<ConceptSet> run(DeleteQuery query);

    <T extends Answer> Stream<T> run(AggregateQuery<T> query);

    <T extends Answer> Stream<T> run(ComputeQuery<T> query);
}

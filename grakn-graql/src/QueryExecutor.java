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

package grakn.core;

import grakn.core.graql.AggregateQuery;
import grakn.core.graql.ComputeQuery;
import grakn.core.graql.DefineQuery;
import grakn.core.graql.DeleteQuery;
import grakn.core.graql.GetQuery;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.UndefineQuery;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;

import java.util.stream.Stream;

/**
 * Interface for executing queries and getting a result. Examples of possible implementations are: running the query
 * against a tinkerpop graph, or sending the query to some server to execute via gRPC or a REST API.
 *
 * This class allows us to decouple query representation (in {@link grakn.core.graql.Query}) from query execution
 * (here in {@link QueryExecutor}).
 */
public interface QueryExecutor {

    Stream<ConceptMap> run(DefineQuery query);

    Stream<ConceptMap> run(UndefineQuery query);

    Stream<ConceptMap> run(GetQuery query);

    Stream<ConceptMap> run(InsertQuery query);

    Stream<ConceptSet> run(DeleteQuery query);

    <T extends Answer> Stream<T> run(AggregateQuery<T> query);

    <T extends Answer> ComputeExecutor<T> run(ComputeQuery<T> query);
}

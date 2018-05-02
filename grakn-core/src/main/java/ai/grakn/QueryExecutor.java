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

import ai.grakn.concept.Concept;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Interface for executing queries and getting a result. Examples of possible implementations are: running the query
 * against a tinkerpop graph, or sending the query to some server to execute via gRPC or a REST API.
 *
 * <p>
 *     This class allows us to decouple query representation (in {@link ai.grakn.graql.Query}) from query execution
 *     (here in {@link QueryExecutor}).
 * </p>
 *
 * @author Felix Chapman
 */
public interface QueryExecutor {

    Stream<Answer> run(GetQuery query);

    Stream<Answer> run(InsertQuery query);

    void run(DeleteQuery query);

    Answer run(DefineQuery query);

    void run(UndefineQuery query);

    <T> T run(AggregateQuery<T> query);

    ComputeJob<ComputeAnswer> run(NewComputeQuery query);

    <T> ComputeJob<T> run(ConnectedComponentQuery<T> query);

    ComputeJob<Map<Long, Set<String>>> run(CorenessQuery query);

    ComputeJob<Long> run(CountQuery query);

    ComputeJob<Map<Long, Set<String>>> run(DegreeQuery query);

    ComputeJob<Map<String, Set<String>>> run(KCoreQuery query);

    ComputeJob<Optional<Number>> run(MaxQuery query);

    ComputeJob<Optional<Double>> run(MeanQuery query);

    ComputeJob<Optional<Number>> run(MedianQuery query);

    ComputeJob<Optional<Number>> run(MinQuery query);

    ComputeJob<List<List<Concept>>> run(PathQuery query);

    ComputeJob<Optional<Double>> run(StdQuery query);

    ComputeJob<Optional<Number>> run(SumQuery query);
}

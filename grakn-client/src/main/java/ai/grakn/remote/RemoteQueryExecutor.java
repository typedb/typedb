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

package ai.grakn.remote;

import ai.grakn.ComputeJob;
import ai.grakn.QueryExecutor;
import ai.grakn.concept.Concept;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
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
import ai.grakn.grpc.GrpcClient;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Remote implementation of {@link QueryExecutor} that communicates with a Grakn server using gRPC.
 *
 * <p>
 *     Like {@link RemoteGraknTx}, this class is an adapter that uses the {@link GrpcClient} for gRPC calls.
 * </p>
 *
 * @author Felix Chapman
 */
final class RemoteQueryExecutor implements QueryExecutor {

    private final GrpcClient client;

    private RemoteQueryExecutor(GrpcClient client) {
        this.client = client;
    }

    public static RemoteQueryExecutor create(GrpcClient client) {
        return new RemoteQueryExecutor(client);
    }

    @Override
    public Stream<Answer> run(GetQuery query) {
        return runAnswerStream(query);
    }

    @Override
    public Stream<Answer> run(InsertQuery query) {
        return runAnswerStream(query);
    }

    @Override
    public void run(DeleteQuery query) {
        runVoid(query);
    }

    @Override
    public Answer run(DefineQuery query) {
        return runSingle(query, Answer.class);
    }

    @Override
    public void run(UndefineQuery query) {
        runVoid(query);
    }

    @Override
    public <T> T run(AggregateQuery<T> query) {
        return runSingleUnchecked(query);
    }

    @Override
    public ComputeJob<ComputeAnswer> run(NewComputeQuery query) {
        return null; //TODO: implement run(NewComputeQuery) for RemoteQueryExecutor
    }
    @Override
    public <T> ComputeJob<T> run(ConnectedComponentQuery<T> query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Map<Long, Set<String>>> run(CorenessQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Long> run(CountQuery query) {
        return runCompute(query, Long.class);
    }

    @Override
    public ComputeJob<Map<Long, Set<String>>> run(DegreeQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Map<String, Set<String>>> run(KCoreQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MaxQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Double>> run(MeanQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MedianQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(MinQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<List<List<Concept>>> run(PathQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Double>> run(StdQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<Optional<Number>> run(SumQuery query) {
        return runComputeUnchecked(query);
    }

    private Iterator<Object> run(Query<?> query) {
        return client.execQuery(query);
    }

    private void runVoid(Query<?> query) {
        run(query).forEachRemaining(empty -> {});
    }

    private Stream<Answer> runAnswerStream(Query<?> query) {
        Iterable<Object> iterable = () -> run(query);
        Stream<Object> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.map(Answer.class::cast);
    }

    private <T> ComputeJob<T> runCompute(ComputeQuery<? extends T> query, Class<? extends T> clazz) {
        return RemoteComputeJob.of(runSingle(query, clazz));
    }

    private <T> ComputeJob<T> runComputeUnchecked(ComputeQuery<? extends T> query) {
        return RemoteComputeJob.of(runSingleUnchecked(query));
    }

    private <T> T runSingle(Query<? extends T> query, Class<? extends T> clazz) {
        return clazz.cast(Iterators.getOnlyElement(run(query)));
    }

    private <T> T runSingleUnchecked(Query<? extends T> query) {
        return (T) Iterators.getOnlyElement(run(query));
    }
}

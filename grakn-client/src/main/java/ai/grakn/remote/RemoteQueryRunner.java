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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.ComputeJob;
import ai.grakn.QueryRunner;
import ai.grakn.concept.Concept;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Remote implementation of {@link QueryRunner} that communicates with a Grakn server using gRPC.
 *
 * <p>
 *     Like {@link RemoteGraknTx}, this class is an adapter that uses the {@link GrpcClient} for gRPC calls.
 * </p>
 *
 * @author Felix Chapman
 */
final class RemoteQueryRunner implements QueryRunner {

    private final RemoteGraknTx tx;
    private final GrpcClient client;
    private final @Nullable Boolean infer;

    private RemoteQueryRunner(RemoteGraknTx tx, GrpcClient client, @Nullable Boolean infer) {
        this.tx = tx;
        this.client = client;
        this.infer = infer;
    }

    public static RemoteQueryRunner create(RemoteGraknTx tx, GrpcClient client, @Nullable Boolean infer) {
        return new RemoteQueryRunner(tx, client, infer);
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
    public <T> ComputeJob<T> run(ClusterQuery<T> query) {
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
    public ComputeJob<Optional<List<Concept>>> run(PathQuery query) {
        return runComputeUnchecked(query);
    }

    @Override
    public ComputeJob<List<List<Concept>>> run(PathsQuery query) {
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
        return client.execQuery(tx, query, infer);
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

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

package ai.grakn.client.executor;

import ai.grakn.ComputeExecutor;
import ai.grakn.QueryExecutor;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.client.Grakn;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Remote implementation of {@link QueryExecutor} that communicates with a Grakn server using gRPC.
 */
public final class RemoteQueryExecutor implements QueryExecutor {

    private final Grakn.Transaction tx;

    private RemoteQueryExecutor(Grakn.Transaction tx) {
        this.tx = tx;
    }

    public static RemoteQueryExecutor create(Grakn.Transaction tx) {
        return new RemoteQueryExecutor(tx);
    }

    @Override
    public Stream<ConceptMap> run(GetQuery query) {
        return streamConceptMaps(query);
    }

    @Override
    public Stream<ConceptMap> run(InsertQuery query) {
        return streamConceptMaps(query);
    }

    @Override
    public void run(DeleteQuery query) {
        runVoid(query);
    }

    @Override
    public ConceptMap run(DefineQuery query) {
        return (ConceptMap) Iterators.getOnlyElement(tx.query(query));
    }

    @Override
    public void run(UndefineQuery query) {
        runVoid(query);
    }

    @Override
    public <T> T run(AggregateQuery<T> query) {
        Iterator iterator = tx.query(query);
        if (iterator.hasNext()) return (T) Iterators.getOnlyElement(iterator);
        else return null;
    }

    @Override
    public <T extends Answer> ComputeExecutor<T> run(ComputeQuery<T> query) {
        Iterable<T> iterable = () -> tx.query(query);
        Stream<T> stream = StreamSupport.stream(iterable.spliterator(), false);
        return RemoteComputeExecutor.of(stream);
    }

    private void runVoid(Query<?> query) {
        tx.query(query).forEachRemaining(empty -> {});
    }

    private Stream<ConceptMap> streamConceptMaps(Query<?> query) {
        Iterable<Object> iterable = () -> tx.query(query);
        Stream<Object> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.map(ConceptMap.class::cast);
    }
}

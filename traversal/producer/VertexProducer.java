/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.traversal.producer;

import grakn.common.collection.Collections;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.ProcedureVertex;

import java.util.concurrent.CompletableFuture;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;
import static java.util.concurrent.CompletableFuture.runAsync;

public class VertexProducer implements Producer<VertexMap> {

    private final ResourceIterator<VertexMap> iterator;
    private CompletableFuture<Void> future;

    public VertexProducer(GraphManager graphMgr, ProcedureVertex<?, ?> vertex, Traversal.Parameters parameters) {
        assert vertex.id().isNamedReference();
        this.iterator = vertex.iterator(graphMgr, parameters).map(
                v -> VertexMap.of(Collections.map(pair(vertex.id().asVariable().reference(), v)))
        );
    }

    @Override
    public void produce(Sink<VertexMap> sink, int count) {
        if (future == null) future = runAsync(consume(count, sink), forkJoinPool());
        else future.thenRun(consume(count, sink));
    }

    private Runnable consume(int count, Sink<VertexMap> sink) {
        return () -> {
            int i = 0;
            for (; i < count; i++) {
                if (iterator.hasNext()) sink.put(iterator.next());
                else break;
            }
            if (i < count) sink.done(this);
        };
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

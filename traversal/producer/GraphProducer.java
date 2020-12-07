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
 *
 */

package grakn.core.traversal.producer;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.iterator.SynchronisedIterator;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.Procedure;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;
import static grakn.core.common.iterator.Iterators.synchronised;
import static java.util.concurrent.CompletableFuture.runAsync;

public class GraphProducer implements Producer<VertexMap> {

    private final int parallelisation;
    private final GraphManager graphMgr;
    private final Procedure procedure;
    private final Traversal.Parameters parameters;
    private final SynchronisedIterator<? extends Vertex<?, ?>> start;
    private final ConcurrentMap<ResourceIterator<VertexMap>, CompletableFuture<Void>> futures;
    private final ConcurrentHashMap.KeySetView<VertexMap, Boolean> produced;
    private final AtomicBoolean isDone;

    public GraphProducer(GraphManager graphMgr, Procedure procedure, Traversal.Parameters parameters, int parallelisation) {
        assert parallelisation > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.parameters = parameters;
        this.parallelisation = parallelisation;
        this.isDone = new AtomicBoolean(false);
        this.futures = new ConcurrentHashMap<>();
        this.produced = ConcurrentHashMap.newKeySet();
        this.start = synchronised(procedure.startVertex().iterator(graphMgr, parameters));
    }

    @Override
    public void produce(Sink<VertexMap> sink, int count) {
        int p = futures.isEmpty() ? parallelisation : futures.size();
        int splitCount = (int) Math.ceil((double) count / p);

        if (futures.isEmpty()) {
            if (!start.hasNext()) {
                done(sink);
                return;
            }

            int i = 0;
            for (; i < parallelisation && start.hasNext(); i++) {
                ResourceIterator<VertexMap> iterator = new GraphIterator(graphMgr, start.next(), procedure, parameters).distinct(produced);
                futures.computeIfAbsent(iterator, k -> runAsync(consume(iterator, splitCount, sink), forkJoinPool()));
            }
            if (i < parallelisation) produce(sink, (parallelisation - i) * splitCount);
        } else {
            for (ResourceIterator<VertexMap> iterator : futures.keySet()) {
                futures.computeIfPresent(iterator, (k, v) -> v.thenRun(consume(k, splitCount, sink)));
            }
        }
    }

    private Runnable consume(ResourceIterator<VertexMap> iterator, int count, Sink<VertexMap> sink) {
        return () -> {
            int i = 0;
            for (; i < count; i++) {
                if (iterator.hasNext()) sink.put(iterator.next());
                else break;
            }
            if (i < count) compensate(iterator, count - i, sink);
        };
    }

    private void compensate(ResourceIterator<VertexMap> completedIterator, int remaining, Sink<VertexMap> sink) {
        futures.remove(completedIterator);
        Vertex<?, ?> next;
        if ((next = start.atomicNext()) != null) {
            ResourceIterator<VertexMap> iterator = new GraphIterator(graphMgr, next, procedure, parameters).distinct(produced);
            futures.put(iterator, runAsync(consume(iterator, remaining, sink), forkJoinPool()));
        } else if (futures.isEmpty()) {
            done(sink);
        } else {
            produce(sink, remaining);
        }
    }

    private void done(Sink<VertexMap> sink) {
        if (isDone.compareAndSet(false, true)) {
            sink.done(this);
        }
    }

    @Override
    public void recycle() {
        start.recycle();
        futures.keySet().forEach(ResourceIterator::recycle);
    }
}

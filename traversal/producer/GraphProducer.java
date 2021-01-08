/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;

public class GraphProducer implements Producer<VertexMap> {

    private final int parallelisation;
    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final ResourceIterator<? extends Vertex<?, ?>> start;
    private final ConcurrentHashMap.KeySetView<VertexMap, Boolean> produced;
    private final AtomicBoolean isDone;
    private final Map<ResourceIterator<VertexMap>, CompletableFuture<Void>> futures;

    public GraphProducer(GraphManager graphMgr, GraphProcedure procedure, Traversal.Parameters params, int parallelisation) {
        assert parallelisation > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.parallelisation = parallelisation;
        this.isDone = new AtomicBoolean(false);
        this.produced = ConcurrentHashMap.newKeySet();
        this.start = procedure.startVertex().iterator(graphMgr, params);
        this.futures = new HashMap<>();
    }

    @Override
    public synchronized void produce(Sink<VertexMap> sink, int count) {
        if (futures.size() < parallelisation) {
            for (int i = futures.size(); i < parallelisation && start.hasNext(); i++) {
                ResourceIterator<VertexMap> iterator =
                        new GraphIterator(graphMgr, start.next(), procedure, params).distinct(produced);
                futures.put(iterator, CompletableFuture.completedFuture(null));
            }
        }
        if (futures.size() == 0) {
            done(sink);
        } else {
            int splitCount = (int) Math.ceil((double) count / futures.size());
            for (ResourceIterator<VertexMap> iterator : futures.keySet()) {
                futures.computeIfPresent(iterator,
                        (k, v) -> v.thenRunAsync(() -> consume(sink, k, splitCount), forkJoinPool())
                );
            }
        }
    }

    private synchronized void finish(ResourceIterator<VertexMap> iterator) {
        futures.remove(iterator);
    }

    private void consume(Sink<VertexMap> sink, ResourceIterator<VertexMap> iterator, int count) {
        try {
            int i = 0;
            for (; i < count && iterator.hasNext(); i++) sink.put(iterator.next());
            finish(iterator);
            if (count - i > 0) produce(sink, count - i);
        } catch (Throwable e) {
            done(sink, e);
        }
    }

    private void done(Sink<VertexMap> sink) {
        if (isDone.compareAndSet(false, true)) {
            sink.done(this);
        }
    }

    private void done(Sink<VertexMap> sink, Throwable e) {
        if (isDone.compareAndSet(false, true)) {
            sink.done(this, e);
        }
    }

    @Override
    public void recycle() {
        start.recycle();
        futures.keySet().forEach(ResourceIterator::recycle);
    }
}

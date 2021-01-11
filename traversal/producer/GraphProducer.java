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
import static java.util.concurrent.CompletableFuture.runAsync;

public class GraphProducer implements Producer<VertexMap> {

    private final int parallelisation;
    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final ResourceIterator<? extends Vertex<?, ?>> start;
    private final ConcurrentHashMap.KeySetView<VertexMap, Boolean> produced;
    private final AtomicBoolean isDone;
    private final Map<ResourceIterator<VertexMap>, CompletableFuture<Void>> futures;
    private final Map<ResourceIterator<VertexMap>, Integer> jobs;

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
        this.jobs = new HashMap<>();
    }

    @Override
    public synchronized void produce(Queue<VertexMap> queue, int count) {
        if (jobs.size() < parallelisation) {
            for (int i = jobs.size(); i < parallelisation && start.hasNext(); i++) {
                ResourceIterator<VertexMap> iterator =
                        new GraphIterator(graphMgr, start.next(), procedure, params).distinct(produced);
                jobs.put(iterator, 0);
            }
        }
        if (jobs.size() == 0) done(queue);
        else {
            int splitCount = (int) Math.ceil((double) count / jobs.size());
            for (ResourceIterator<VertexMap> iterator : jobs.keySet()) {
                jobs.computeIfPresent(iterator, (k, v) -> v + splitCount);
                if (!futures.containsKey(iterator))
                    futures.put(iterator, runAsync(() -> produceAsync(queue, iterator), forkJoinPool()));
            }
        }
    }

    private synchronized int take(ResourceIterator<VertexMap> iterator) {
        int count = jobs.get(iterator);
        jobs.put(iterator, 0);
        return count;
    }

    private void produceAsync(Queue<VertexMap> queue, ResourceIterator<VertexMap> iterator) {
        try {
            int toConsume = 0;
            int consumed = 0;
            while (iterator.hasNext()) {
                toConsume = take(iterator);
                for (consumed = 0; consumed < toConsume && iterator.hasNext(); consumed++)
                    queue.put(iterator.next());
            }
            synchronized (this) {
                futures.remove(iterator);
                int toCompensate = toConsume - consumed + take(iterator);
                if (!iterator.hasNext()) jobs.remove(iterator);
                if (toCompensate > 0) produce(queue, toCompensate);
            }
        } catch (Throwable e) {
            done(queue, e);
        }
    }

    private void done(Queue<VertexMap> queue) {
        if (isDone.compareAndSet(false, true)) {
            queue.done(this);
        }
    }

    private void done(Queue<VertexMap> queue, Throwable e) {
        if (isDone.compareAndSet(false, true)) {
            queue.done(this, e);
        }
    }

    @Override
    public void recycle() {
        start.recycle();
        futures.keySet().forEach(ResourceIterator::recycle);
    }
}

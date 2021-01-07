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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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
    private final Map<ResourceIterator<VertexMap>, Semaphore> jobs;
    private final List<ResourceIterator<VertexMap>> toRecycle;

    public GraphProducer(GraphManager graphMgr, GraphProcedure procedure, Traversal.Parameters params, int parallelisation) {
        assert parallelisation > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.parallelisation = parallelisation;
        this.isDone = new AtomicBoolean(false);
        this.produced = ConcurrentHashMap.newKeySet();
        this.start = procedure.startVertex().iterator(graphMgr, params);
        this.jobs = new HashMap<>();
        this.toRecycle = new ArrayList<>();
    }

    private synchronized void assign(Sink<VertexMap> sink, int count) {
        if (jobs.size() < parallelisation) {
            for (int i = jobs.size(); i < parallelisation && start.hasNext(); i++) {
                ResourceIterator<VertexMap> iterator = new GraphIterator(graphMgr, start.next(), procedure, params).distinct(produced);
                jobs.put(iterator, new Semaphore(0));
                runAsync(() -> consume(sink, iterator), forkJoinPool());
            }
        }
        if (jobs.size() == 0) {
            if (isDone.compareAndSet(false, true)) {
                sink.done(this);
                return;
            }
        }
        int splitCount = (int) Math.ceil((double) count / jobs.size());
        for (ResourceIterator<VertexMap> iterator : jobs.keySet()) {
            jobs.get(iterator).release(splitCount);
        }
    }

    private synchronized void unassign(ResourceIterator<VertexMap> iterator) {
        jobs.remove(iterator);
        toRecycle.add(iterator);
    }

    private void consume(Sink<VertexMap> sink, ResourceIterator<VertexMap> iterator) {
        try {
            Semaphore sema = jobs.get(iterator);
            while (iterator.hasNext()) {
                sema.acquire();
                sink.put(iterator.next());
            }
            unassign(iterator);
            if (sema.availablePermits() > 0) {
                assign(sink, sema.availablePermits());
            }
        } catch (Throwable e) {
            if (isDone.compareAndSet(false, true)) {
                sink.done(this, e);
            }
        }
    }

    @Override
    public void produce(Sink<VertexMap> sink, int count) {
        assign(sink, count);
    }

    @Override
    public void recycle() {
        start.recycle();
        toRecycle.forEach(iterator -> iterator.recycle());
        toRecycle.clear();
    }
}

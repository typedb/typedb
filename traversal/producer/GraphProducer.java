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
    private final Map<ResourceIterator<VertexMap>, CompletableFuture<Void>> jobs;
    private final Map<ResourceIterator<VertexMap>, Integer> jobsToProduce;

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
        this.jobsToProduce = new HashMap<>();
    }

    @Override
    public synchronized void produce(Queue<VertexMap> queue, int count) {
        if (jobsToProduce.size() < parallelisation) {
            for (int i = jobsToProduce.size(); i < parallelisation && start.hasNext(); i++) {
                ResourceIterator<VertexMap> iterator =
                        new GraphIterator(graphMgr, start.next(), procedure, params).distinct(produced);
                jobsToProduce.put(iterator, 0);
            }
        }
        if (jobsToProduce.size() == 0) done(queue);
        else {
            int splitCount = (int) Math.ceil((double) count / jobsToProduce.size());
            for (ResourceIterator<VertexMap> iterator : jobsToProduce.keySet()) {
                jobsToProduce.computeIfPresent(iterator, (k, v) -> v + splitCount);
                if (!jobs.containsKey(iterator))
                    jobs.put(iterator, runAsync(() -> produceAsync(queue, iterator), forkJoinPool()));
            }
        }
    }

    private synchronized int take(ResourceIterator<VertexMap> iterator) {
        int count = jobsToProduce.get(iterator);
        jobsToProduce.put(iterator, 0);
        return count;
    }

    private synchronized void compensate(Queue<VertexMap> queue, ResourceIterator<VertexMap> iterator, int toProduce) {
        jobs.remove(iterator);
        int toCompensate = toProduce + take(iterator);
        if (!iterator.hasNext()) jobsToProduce.remove(iterator);
        if (toCompensate > 0) produce(queue, toCompensate);
    }

    private void produceAsync(Queue<VertexMap> queue, ResourceIterator<VertexMap> iterator) {
        try {
            int toProduce = 0;
            int produced = 0;
            while (iterator.hasNext()) {
                toProduce = take(iterator);
                produced = 0;
                if (toProduce > 0) {
                    for (; produced < toProduce && iterator.hasNext(); produced++)
                        queue.put(iterator.next());
                } else break;
            }
            compensate(queue, iterator, toProduce - produced);
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
        jobs.keySet().forEach(ResourceIterator::recycle);
    }
}

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
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;

@ThreadSafe
public class GraphProducer implements Producer<VertexMap> {

    private final int parallelisation;
    private final GraphManager graphMgr;
    private final GraphProcedure procedure;
    private final Traversal.Parameters params;
    private final List<Identifier.Variable.Name> filter;
    private final ResourceIterator<? extends Vertex<?, ?>> start;
    private final ConcurrentHashMap.KeySetView<VertexMap, Boolean> produced;
    private final Map<ResourceIterator<VertexMap>, CompletableFuture<Void>> runningJobs;
    private final AtomicBoolean isDone;
    private boolean isInitialised;

    public GraphProducer(GraphManager graphMgr, GraphProcedure procedure, Traversal.Parameters params,
                         List<Identifier.Variable.Name> filter, int parallelisation) {
        assert parallelisation > 0;
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.params = params;
        this.filter = filter;
        this.parallelisation = parallelisation;
        this.runningJobs = new HashMap<>();
        this.produced = ConcurrentHashMap.newKeySet();
        this.start = procedure.startVertex().iterator(graphMgr, params);
        this.isDone = new AtomicBoolean(false);
        this.isInitialised = false;
    }

    @Override
    public synchronized void produce(Producer.Queue<VertexMap> queue, int request) {
        if (isDone.get()) return;
        else if (!isInitialised) initialise(queue);
        distribute(queue, request);
    }

    private synchronized void initialise(Queue<VertexMap> queue) {
        for (int i = 0; i < parallelisation && start.hasNext(); i++) {
            ResourceIterator<VertexMap> iter =
                    new GraphIterator(graphMgr, start.next(), procedure, params, filter).distinct(produced);
            runningJobs.put(iter, CompletableFuture.completedFuture(null));
        }
        isInitialised = true;
        if (runningJobs.isEmpty()) done(queue);
    }

    private synchronized void distribute(Queue<VertexMap> queue, int request) {
        if (isDone.get()) return;
        int requestSplitMax = (int) Math.ceil((double) request / runningJobs.size());
        int requestSent = 0;
        for (ResourceIterator<VertexMap> iterator : runningJobs.keySet()) {
            int requestSplit = Math.min(requestSplitMax, request - requestSent);
            runningJobs.computeIfPresent(iterator, (iter, asyncJob) -> asyncJob.thenRunAsync(
                    () -> job(queue, iter, requestSplit), forkJoinPool()
            ));
            requestSent += requestSplit;
            if (requestSent == request) break;
        }
    }

    private synchronized void transition(Queue<VertexMap> queue, ResourceIterator<VertexMap> iterator, int unfulfilled) {
        if (iterator.hasNext()) {
            assert unfulfilled == 0;
            return;
        }

        runningJobs.remove(iterator);
        if (start.hasNext()) {
            ResourceIterator<VertexMap> newIter =
                    new GraphIterator(graphMgr, start.next(), procedure, params, filter).distinct(produced);
            CompletableFuture<Void> asyncJob = unfulfilled > 0
                    ? CompletableFuture.runAsync(() -> job(queue, newIter, unfulfilled), forkJoinPool())
                    : CompletableFuture.runAsync(() -> {}, forkJoinPool());
            runningJobs.put(newIter, asyncJob);
        } else if (!runningJobs.isEmpty() && unfulfilled > 0) {
            distribute(queue, unfulfilled);
        } else if (runningJobs.isEmpty()) {
            done(queue);
        }
    }

    private void job(Queue<VertexMap> queue, ResourceIterator<VertexMap> iterator, int request) {
        try {
            int i = 0;
            for (; i < request && iterator.hasNext() && !isDone.get(); i++) queue.put(iterator.next());
            if (!isDone.get()) transition(queue, iterator, request - i);
        } catch (Throwable e) {
            done(queue, e);
        }
    }

    private void done(Queue<VertexMap> queue) {
        if (isDone.compareAndSet(false, true)) {
            queue.done();
        }
    }

    private void done(Queue<VertexMap> queue, Throwable e) {
        if (isDone.compareAndSet(false, true)) {
            queue.done(e);
        }
    }

    @Override
    public synchronized void recycle() {
        start.recycle();
        runningJobs.keySet().forEach(ResourceIterator::recycle);
    }
}

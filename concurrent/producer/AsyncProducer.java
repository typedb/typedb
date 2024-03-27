/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.producer;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static java.util.concurrent.CompletableFuture.completedFuture;

@ThreadSafe
public class AsyncProducer<T> implements FunctionalProducer<T> {

    private final int parallelisation;
    private final FunctionalIterator<FunctionalIterator<T>> iterators;
    private final ConcurrentMap<FunctionalIterator<T>, CompletableFuture<Void>> runningJobs;
    private final AtomicBoolean isDone;
    private final ReadWriteLock recycleLock;
    private volatile boolean isRecycled;
    private boolean isInitialised;

    AsyncProducer(FunctionalIterator<FunctionalIterator<T>> iterators, int parallelisation) {
        assert parallelisation > 0;
        this.iterators = iterators;
        this.parallelisation = parallelisation;
        this.runningJobs = new ConcurrentHashMap<>();
        this.isDone = new AtomicBoolean(false);
        this.isInitialised = false;
        this.recycleLock = new ReentrantReadWriteLock();
        this.isRecycled = false;
    }

    @Override
    public <U> AsyncProducer<U> map(Function<T, U> mappingFn) {
        return new AsyncProducer<>(iterators.map(iter -> iter.map(mappingFn)), parallelisation);
    }

    @Override
    public AsyncProducer<T> filter(Predicate<T> predicate) {
        return new AsyncProducer<>(iterators.map(iter -> iter.filter(predicate)), parallelisation);
    }

    @Override
    public AsyncProducer<T> distinct() {
        ConcurrentSet<T> produced = new ConcurrentSet<>();
        return new AsyncProducer<>(iterators.map(iter -> iter.distinct(produced)), parallelisation);
    }

    @Override
    public synchronized void produce(Queue<T> queue, int request, Executor executor) {
        if (isDone.get()) return;
        else if (!isInitialised) initialise(queue);
        distribute(queue, request, executor);
    }

    private synchronized void initialise(Queue<T> queue) {
        for (int i = 0; i < parallelisation && iterators.hasNext(); i++) {
            runningJobs.put(iterators.next(), completedFuture(null));
        }
        isInitialised = true;
        if (runningJobs.isEmpty()) done(queue);
    }

    private synchronized void distribute(Queue<T> queue, int request, Executor executor) {
        if (isDone.get()) return;
        int requestSplitMax = (int) Math.ceil((double) request / runningJobs.size());
        int requestSent = 0;
        for (FunctionalIterator<T> iterator : runningJobs.keySet()) {
            int requestSplit = Math.min(requestSplitMax, request - requestSent);
            runningJobs.computeIfPresent(iterator, (iter, asyncJob) -> asyncJob.thenRunAsync(
                    () -> job(queue, iter, requestSplit, executor), executor
            ));
            requestSent += requestSplit;
            if (requestSent == request) break;
        }
    }

    private synchronized void transition(Queue<T> queue, FunctionalIterator<T> iterator, int unfulfilled, Executor executor) {
        if (isRecycled) done(queue);
        else if (!iterator.hasNext()) {
            if (runningJobs.remove(iterator) != null && iterators.hasNext()) compensate(queue, unfulfilled, executor);
            else if (!runningJobs.isEmpty() && unfulfilled > 0) distribute(queue, unfulfilled, executor);
            else if (runningJobs.isEmpty()) done(queue);
            else if (unfulfilled != 0) throw TypeDBException.of(ILLEGAL_STATE);
        } else {
            if (unfulfilled != 0) throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private synchronized void compensate(Queue<T> queue, int unfulfilled, Executor executor) {
        FunctionalIterator<T> it = iterators.next();
        runningJobs.put(it, completedFuture(null));
        if (unfulfilled > 0) {
            runningJobs.computeIfPresent(it, (i, job) -> job.thenRunAsync(
                    () -> job(queue, it, unfulfilled, executor), executor
            ));
        }
    }

    private void job(Queue<T> queue, FunctionalIterator<T> iterator, int request, Executor executor) {
        try {
            int unfulfilled = request;
            if (runningJobs.containsKey(iterator)) {
                for (; unfulfilled > 0 && !isDone.get(); unfulfilled--) {
                    try {
                        recycleLock.readLock().lock();
                        if (isRecycled) {
                            done(queue);
                            return;
                        }
                        if (iterator.hasNext()) queue.put(iterator.next());
                        else break;
                    } finally {
                        recycleLock.readLock().unlock();
                    }
                }
            }
            if (!isDone.get()) transition(queue, iterator, unfulfilled, executor);
        } catch (Throwable e) {
            done(queue, e);
        }
    }

    private void done(Queue<T> queue) {
        if (isDone.compareAndSet(false, true)) {
            queue.done();
        }
    }

    private void done(Queue<T> queue, Throwable e) {
        if (isDone.compareAndSet(false, true)) {
            queue.done(e);
        }
    }

    @Override
    public synchronized void recycle() {
        try {
            recycleLock.writeLock().lock();
            iterators.recycle();
            runningJobs.keySet().forEach(FunctionalIterator::recycle);
            isRecycled = true;
        } finally {
            recycleLock.writeLock().unlock();
        }
    }
}

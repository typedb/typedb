/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.concurrent.producer;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

@ThreadSafe
public class BaseProducer<T> implements FunctionalProducer<T> {

    private final FunctionalIterator<T> iterator;
    private final AtomicBoolean isDone;
    private CompletableFuture<Void> future;

    BaseProducer(FunctionalIterator<T> iterator) {
        this.iterator = iterator;
        this.isDone = new AtomicBoolean(false);
        this.future = CompletableFuture.completedFuture(null);
    }

    @Override
    public <U> BaseProducer<U> map(Function<T, U> mappingFn) {
        return new BaseProducer<>(iterator.map(mappingFn));
    }

    @Override
    public BaseProducer<T> filter(Predicate<T> predicate) {
        return new BaseProducer<>(iterator.filter(predicate));
    }

    @Override
    public BaseProducer<T> distinct() {
        return new BaseProducer<>(iterator.distinct());
    }

    @Override
    public synchronized void produce(Queue<T> queue, int request, Executor executor) {
        if (isDone.get()) return;
        future = future.thenRunAsync(() -> {
            try {
                int unfulfilled = request;
                for (; unfulfilled > 0 && iterator.hasNext() && !isDone.get(); unfulfilled--)
                    queue.put(iterator.next());
                if (unfulfilled > 0 && !isDone.get()) done(queue);
            } catch (Throwable e) {
                queue.done(e);
            }
        }, executor);
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
        iterator.recycle();
    }
}

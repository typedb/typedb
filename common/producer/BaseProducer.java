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

package grakn.core.common.producer;

import grakn.core.common.iterator.ResourceIterator;

import java.util.concurrent.CompletableFuture;

import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;

public class BaseProducer<T> implements Producer<T> {

    private final ResourceIterator<T> iterator;
    private CompletableFuture<Void> future;

    BaseProducer(ResourceIterator<T> iterator) {
        this.iterator = iterator;
        this.future = CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void produce(Queue<T> queue, int count) {
        future = future.thenRunAsync(() -> produceAsync(queue, count), forkJoinPool());
    }

    private void produceAsync(Queue<T> queue, int count) {
        try {
            for (int i = 0; i < count; i++) {
                if (iterator.hasNext()) {
                    queue.put(iterator.next());
                } else {
                    queue.done(this);
                    break;
                }
            }
        } catch (Throwable e) {
            queue.done(this, e);
        }
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

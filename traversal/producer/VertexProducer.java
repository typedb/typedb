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
 */

package grakn.core.traversal.producer;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.traversal.common.VertexMap;

import java.util.concurrent.CompletableFuture;

import static grakn.core.common.concurrent.ExecutorService.forkJoinPool;
import static java.util.concurrent.CompletableFuture.runAsync;

public class VertexProducer implements Producer<VertexMap> {

    private final ResourceIterator<VertexMap> iterator;
    private CompletableFuture<Void> future;

    public VertexProducer(ResourceIterator<VertexMap> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void produce(Queue<VertexMap> queue, int count) {
        if (future == null) {
            future = runAsync(consume(count, queue), forkJoinPool());
        } else future.thenRun(consume(count, queue));
    }

    private Runnable consume(int count, Queue<VertexMap> sink) {
        return () -> {
            try {
                int i = 0;
                for (; i < count; i++) {
                    if (iterator.hasNext()) sink.put(iterator.next());
                    else break;
                }
                if (i < count) sink.done(this);
            } catch (Throwable e) {
                sink.done(this, e);
            }
        };
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

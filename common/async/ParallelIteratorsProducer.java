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
 */

package grakn.core.common.async;

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.concurrent.ManagedBlockingQueue;
import grakn.core.common.iterator.ResourceIterator;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ParallelIteratorsProducer<T> extends Producer<T> {

    private static int DOWNSTREAM_REQUEST_BATCH = 1;

    private final AtomicLong requestedFromUpstream;
    private final AtomicBoolean responding;
    private final Set<ResourceIterator<T>> availableIterators;
    private final ManagedBlockingQueue<T> produced;

    public ParallelIteratorsProducer(Consumer<T> onProduce, Runnable onDone, Set<ResourceIterator<T>> iterators) {
        super(onProduce, onDone);
        availableIterators = new HashSet<>();
        availableIterators.addAll(iterators);
        produced = new ManagedBlockingQueue<>();
        requestedFromUpstream = new AtomicLong(0L);
        responding = new AtomicBoolean(false);

        if (this.availableIterators.isEmpty()) {
            onDone.run();
        }
    }

    @Override
    public void next() {
        long requested = requestedFromUpstream.incrementAndGet();
        if (requested > produced.size()) {
            pullFromAvailableIterators();
        }
        ExecutorService.forkJoinPool().submit(this::respond);
    }

    private void respond() {
        if (responding.compareAndSet(false, true)) {
            while (requestedFromUpstream.get() > 0) {
                final T result;
                try {
                    result = produced.take();
                } catch (InterruptedException e) {
                    // TODO better exception after integrating this code into execution engine
                    throw new RuntimeException(e);
                }

                long requested = requestedFromUpstream.decrementAndGet();
                if (requested > produced.size()) pullFromAvailableIterators();
                onAnswer.accept(result);
            }
            responding.set(false);
        }
    }

    private void pullFromDownstream(ResourceIterator<T> iterator) {
        ExecutorService.forkJoinPool().submit(() -> {
            int retrieved = 0;
            while (iterator.hasNext() && retrieved < DOWNSTREAM_REQUEST_BATCH) {
                final T next = iterator.next();
                retrieved++;
                try {
                    produced.put(next);
                } catch (InterruptedException e) {
                    // TODO better exception after integrating this code into execution engine
                    throw new RuntimeException(e);
                }
            }
            if (iterator.hasNext()) {
                iteratorAvailable(iterator);
            } else {
                iteratorExhausted();
            }
        });
    }

    // synchronised to avoid modifying collection during check
    private synchronized void iteratorExhausted() {
        if (availableIterators.isEmpty()) {
            onDone.run();
        }
    }

    // synchronized to avoid modifying collection during iteration
    private synchronized void pullFromAvailableIterators() {
        for (ResourceIterator<T> iterator : availableIterators) {
            pullFromDownstream(iterator);
        }
        availableIterators.clear();
    }

    // synchronized to avoid modifying collection during iteration
    private synchronized void iteratorAvailable(final ResourceIterator<T> iterator) {
        availableIterators.add(iterator);
    }
}

/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class BlockingQueueStream {

    public static <T> Stream<T> streamFromBlockingQueue(BlockingQueue<Optional<T>> queue) {
        return streamFromIterator(iteratorFromBlockingQueue(queue));
    }

    private static <T> Iterator<T> iteratorFromBlockingQueue(BlockingQueue<Optional<T>> queue) {
        return new Iterator<T>() {
            private Optional<T> next = queueNext();

            @Override
            public boolean hasNext() {
                return next.isPresent();
            }

            @Override
            public T next() {
                T result = next.orElseThrow(NoSuchElementException::new);
                next = queueNext();
                return result;
            }

            private Optional<T> queueNext() {
                try {
                    return queue.take();
                } catch (InterruptedException e) {
                    return Optional.empty();
                }
            }
        };
    }

    private static <T> Stream<T> streamFromIterator(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}

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

package grakn.core.concurrent.producer;

import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Function;

@ThreadSafe
public class MappedProducer<T, U> implements Producer<U> {

    private final Producer<T> baseProducer;
    private final Function<T, U> mappingFn;

    MappedProducer(Producer<T> baseProducer, Function<T, U> mappingFn) {
        this.baseProducer = baseProducer;
        this.mappingFn = mappingFn;
    }

    @Override
    public void produce(Producer.Queue<U> queue, int request) {
        baseProducer.produce(new Queue(queue), request);
    }

    @Override
    public void recycle() {
        baseProducer.recycle();
    }

    @ThreadSafe
    private class Queue implements Producer.Queue<T> {

        private final Producer.Queue<U> baseQueue;

        Queue(Producer.Queue<U> baseQueue) {
            this.baseQueue = baseQueue;
        }

        @Override
        public void put(T item) {
            baseQueue.put(mappingFn.apply(item));
        }

        @Override
        public void done() {
            baseQueue.done();
        }

        @Override
        public void done(Throwable e) {
            baseQueue.done(e);
        }
    }
}

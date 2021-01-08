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

import java.util.function.Function;

public class MappedProducer<T, U> implements Producer<U> {

    private final Producer<T> baseProducer;
    private final Function<T, U> mappingFn;

    public MappedProducer(Producer<T> baseProducer, Function<T, U> mappingFn) {
        this.baseProducer = baseProducer;
        this.mappingFn = mappingFn;
    }

    @Override
    public void produce(Producer.Sink<U> sink, int count) {
        baseProducer.produce(new Sink(sink), count);
    }

    @Override
    public void recycle() {
        baseProducer.recycle();
    }

    private class Sink implements Producer.Sink<T> {

        private final Producer.Sink<U> baseSink;

        Sink(Producer.Sink<U> baseSink) {
            this.baseSink = baseSink;
        }

        @Override
        public void put(T item) {
            baseSink.put(mappingFn.apply(item));
        }

        @Override
        public void done(Producer<T> producer) {
            baseSink.done(MappedProducer.this);
        }

        @Override
        public void done(Producer<T> producer, Throwable e) {
            baseSink.done(MappedProducer.this, e);
        }
    }
}

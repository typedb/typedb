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

import java.util.function.Predicate;

public class FilteredProducer<T> implements Producer<T> {

    private final Producer<T> baseProducer;
    private final Predicate<T> predicate;

    public FilteredProducer(Producer<T> baseProducer, Predicate<T> predicate) {
        this.baseProducer = baseProducer;
        this.predicate = predicate;
    }

    @Override
    public void produce(Producer.Sink<T> sink, int count) {
        baseProducer.produce(new Sink(sink), count);
    }

    @Override
    public void recycle() {
        baseProducer.recycle();
    }

    private class Sink implements Producer.Sink<T> {

        private final Producer.Sink<T> baseSink;

        Sink(Producer.Sink<T> baseSink) {
            this.baseSink = baseSink;
        }

        @Override
        public void put(T item) {
            if (predicate.test(item)) baseSink.put(item);
            else baseProducer.produce(this, 1);
        }

        @Override
        public void done(Producer<T> producer) {
            baseSink.done(FilteredProducer.this);
        }

        @Override
        public void done(Producer<T> producer, Throwable e) {
            baseSink.done(FilteredProducer.this, e);
        }
    }
}

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

package grakn.core.common.producer;

import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Function;
import java.util.function.Predicate;

@ThreadSafe
public interface Producer<T> {

    void produce(Queue<T> queue, int request);

    void recycle();

    default <U> Producer<U> map(Function<T, U> mappingFn) {
        return new MappedProducer<>(this, mappingFn);
    }

    default Producer<T> filter(Predicate<T> predicate) {
        return new FilteredProducer<>(this, predicate);
    }

    @ThreadSafe
    interface Queue<U> {

        void put(U item);

        void done();

        void done(Throwable e);
    }
}

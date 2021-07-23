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

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

class FinaliseHandledIterator<T> extends AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Runnable function;

    FinaliseHandledIterator(FunctionalIterator<T> iterator, Runnable function) {
        this.iterator = iterator;
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    @Override
    protected void finalize() {
        function.run();
    }

    static class Sorted<T extends Comparable<? super T>, ITER extends FunctionalIterator.Sorted<T>>
            extends AbstractFunctionalIterator.Sorted<T> {

        private final Runnable function;
        final ITER iterator;
        T last;

        Sorted(ITER iterator, Runnable function) {
            this.iterator = iterator;
            this.function = function;
            this.last = null;
        }

        @Override
        public T peek() {
            return iterator.peek();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            last = iterator.next();
            return last;
        }

        @Override
        public void recycle() {
            iterator.recycle();
        }

        @Override
        protected void finalize() {
            function.run();
        }

        static class Forwardable<T extends Comparable<? super T>>
                extends FinaliseHandledIterator.Sorted<T, FunctionalIterator.Sorted.Forwardable<T>>
                implements FunctionalIterator.Sorted.Forwardable<T> {

            Forwardable(FunctionalIterator.Sorted.Forwardable<T> source, Runnable function) {
                super(source, function);
            }

            @Override
            public void forward(T target) {
                if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
                iterator.forward(target);
            }

            @Override
            public final FunctionalIterator.Sorted.Forwardable<T> merge(FunctionalIterator.Sorted.Forwardable<T> iterator) {
                return Iterators.Sorted.merge(this, iterator);
            }

            @Override
            public <U extends Comparable<? super U>> FunctionalIterator.Sorted.Forwardable<U> mapSorted(
                    Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
                return Iterators.Sorted.mapSorted(this, mappingFn, reverseMappingFn);
            }

            @Override
            public FunctionalIterator.Sorted.Forwardable<T> distinct() {
                return Iterators.Sorted.distinct(this);
            }

            @Override
            public FunctionalIterator.Sorted.Forwardable<T> filter(Predicate<T> predicate) {
                return Iterators.Sorted.filter(this, predicate);
            }
        }
    }
}

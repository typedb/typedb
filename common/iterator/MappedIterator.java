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

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class MappedIterator<T, U> extends AbstractFunctionalIterator<U> {

    private final FunctionalIterator<T> iterator;
    private final Function<T, U> function;

    MappedIterator(FunctionalIterator<T> iterator, Function<T, U> function) {
        this.iterator = iterator;
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public U next() {
        return function.apply(iterator.next());
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    static class Sorted<
            T extends Comparable<? super T>, U extends Comparable<? super U>, ITER extends FunctionalIterator.Sorted<T>
            > extends AbstractFunctionalIterator.Sorted<U> {

        final ITER iterator;
        final Function<T, U> mappingFn;
        State state;
        U next;
        U last;

        enum State {EMPTY, FETCHED, COMPLETED}

        Sorted(ITER iterator, Function<T, U> mappingFn) {
            this.iterator = iterator;
            this.mappingFn = mappingFn;
            this.state = State.EMPTY;
            last = null;
        }

        @Override
        public U peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return next;
        }

        @Override
        public boolean hasNext() {
            switch (state) {
                case COMPLETED:
                    return false;
                case FETCHED:
                    return true;
                case EMPTY:
                    return fetchAndCheck();
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private boolean fetchAndCheck() {
            if (iterator.hasNext()) {
                this.next = mappedNext();
                state = State.FETCHED;
            } else {
                state = State.COMPLETED;
            }
            return state == State.FETCHED;
        }

        U mappedNext() {
            T value = iterator.next();
            return mappingFn.apply(value);
        }

        @Override
        public U next() {
            if (!hasNext()) throw new NoSuchElementException();
            assert last == null || next.compareTo(last) >= 0 : "Sorted mapped iterator produces out of order values";
            last = next;
            state = State.EMPTY;
            return next;
        }

        @Override
        public void recycle() {
            iterator.recycle();
        }

        static class Forwardable<T extends Comparable<? super T>, U extends Comparable<? super U>>
                extends MappedIterator.Sorted<T, U, FunctionalIterator.Sorted.Forwardable<T>>
                implements FunctionalIterator.Sorted.Forwardable<U> {

            private final Function<U, T> reverseMappingFn;

            /**
             * @param source           - iterator to create mapped iterators from
             * @param mappingFn        - The forward mapping function must return a new iterator that is sorted with respect to U's comparator.
             * @param reverseMappingFn - The reverse mapping function must be the able to invert the forward mapping function
             */
            Forwardable(FunctionalIterator.Sorted.Forwardable<T> source, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
                super(source, mappingFn);
                this.reverseMappingFn = reverseMappingFn;
            }

            @Override
            U mappedNext() {
                T value = iterator.next();
                U next = mappingFn.apply(value);
//                assert reverseMappingFn.apply(next).equals(value);
                return next;
            }

            @Override
            public void forward(U target) {
                if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
                T reverseMapped = reverseMappingFn.apply(target);
                iterator.forward(reverseMapped);
                state = State.EMPTY;
            }

            @SafeVarargs
            @Override
            public final FunctionalIterator.Sorted.Forwardable<U> merge(FunctionalIterator.Sorted.Forwardable<U>... iterators) {
                return Iterators.Sorted.merge(this, iterators);
            }

            @Override
            public <V extends Comparable<? super V>> FunctionalIterator.Sorted.Forwardable<V> mapSorted(
                    Function<U, V> mappingFn, Function<V, U> reverseMappingFn) {
                return Iterators.Sorted.mapSorted(this, mappingFn, reverseMappingFn);
            }

            @Override
            public FunctionalIterator.Sorted.Forwardable<U> distinct() {
                return Iterators.Sorted.distinct(this);
            }

            @Override
            public FunctionalIterator.Sorted.Forwardable<U> filter(Predicate<U> predicate) {
                return Iterators.Sorted.filter(this, predicate);
            }

        }
    }
}

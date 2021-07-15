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

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

// TODO: verify (and potentially fix) this class is able to handle null objects
class DistinctIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Set<T> consumed;
    private T next;

    DistinctIterator(FunctionalIterator<T> iterator) {
        this(iterator, new HashSet<>());
    }

    DistinctIterator(FunctionalIterator<T> iterator, Set<T> duplicates) {
        this.iterator = iterator;
        this.consumed = duplicates;
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (iterator.hasNext() && !consumed.add(next = iterator.next())) next = null;
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T result = next;
        next = null;
        return result;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    static class Sorted<T extends Comparable<? super T>, ITER extends FunctionalIterator.Sorted<T>>
            extends AbstractFunctionalIterator.Sorted<T> {

        final ITER iterator;
        T last;

        Sorted(ITER iterator) {
            this.iterator = iterator;
            last = null;
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                if (iterator.peek().equals(last)) iterator.next();
                else return true;
            }
            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            last = iterator.next();
            return last;
        }

        @Override
        public T peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return iterator.peek();
        }

        @Override
        public void recycle() {
            iterator.recycle();
        }

        static class Forwardable<T extends Comparable<? super T>>
                extends DistinctIterator.Sorted<T, FunctionalIterator.Sorted.Forwardable<T>>
                implements FunctionalIterator.Sorted.Forwardable<T> {

            Forwardable(FunctionalIterator.Sorted.Forwardable<T> source) {
                super(source);
            }

            @Override
            public void forward(T target) {
                if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
                iterator.forward(target);
            }

            @SafeVarargs
            @Override
            public final FunctionalIterator.Sorted.Forwardable<T> merge(FunctionalIterator.Sorted.Forwardable<T>... iterators) {
                return Iterators.Sorted.merge(this, iterators);
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

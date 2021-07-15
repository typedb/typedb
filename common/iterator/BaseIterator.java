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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class BaseIterator<T> extends AbstractFunctionalIterator<T> {

    private final Either<FunctionalIterator<T>, Iterator<T>> iterator;

    BaseIterator(Either<FunctionalIterator<T>, Iterator<T>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.apply(Iterator::hasNext, Iterator::hasNext);
    }

    @Override
    public T next() {
        return iterator.apply(Iterator::next, Iterator::next);
    }

    @Override
    public void recycle() {
        iterator.ifFirst(FunctionalIterator::recycle);
    }

    static class Sorted<T extends Comparable<? super T>> extends AbstractFunctionalIterator.Sorted<T>
            implements FunctionalIterator.Sorted.Forwardable<T> {

        private final NavigableSet<T> source;
        private Iterator<T> iterator;
        private T next;
        private T last;

        Sorted(NavigableSet<T> source) {
            this.source = source;
            this.iterator = source.iterator();
            this.last = null;
        }

        @Override
        public boolean hasNext() {
            return (next != null) || fetchAndCheck();
        }

        private boolean fetchAndCheck() {
            if (iterator.hasNext()) {
                next = iterator.next();
                return true;
            } else return false;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            last = next;
            next = null;
            return last;
        }

        @Override
        public T peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return next;
        }

        @Override
        public void forward(T target) {
            if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            this.iterator = source.tailSet(target).iterator();
            this.next = null;
        }

        @SafeVarargs
        @Override
        public final Forwardable<T> merge(Forwardable<T>... iterators) {
            return Iterators.Sorted.merge(this, iterators);
        }

        @Override
        public <U extends Comparable<? super U>> Forwardable<U> mapSorted(
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

        @Override
        public void recycle() { }

    }
}

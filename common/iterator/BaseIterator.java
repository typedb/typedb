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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

class BaseIterator<T> extends AbstractFunctionalIterator<T> {

    private final Either<FunctionalIterator<T>, Iterator<T>> iterator;

    public BaseIterator(Either<FunctionalIterator<T>, Iterator<T>> iterator) {
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

    static class Sorted<T, K extends Comparable<K>> extends AbstractFunctionalIterator.Sorted<T, K> {

        private final Iterator<T> source;
        T next;
        K lastKey;

        public Sorted(Iterator<T> sortedSource, Function<T, K> keyExtractor) {
            super(keyExtractor);
            this.source = sortedSource;
            next = null;
            lastKey = null;
        }

        @Override
        public boolean hasNext() {
            return (next != null) || fetchAndCheck();
        }

        private boolean fetchAndCheck() {
            if (source.hasNext()) {
                next = source.next();
                assert lastKey == null || lastKey.compareTo(keyExtractor().apply(next)) <= 0;
                lastKey = keyExtractor().apply(next);
                return true;
            } else return false;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            T value = next;
            next = null;
            return value;
        }

        @Override
        public T peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return next;
        }

        @Override
        public void recycle() { }
    }
}

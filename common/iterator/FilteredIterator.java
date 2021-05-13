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

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

class FilteredIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Predicate<T> predicate;
    private T next;

    FilteredIterator(FunctionalIterator<T> iterator, Predicate<T> predicate) {
        this.iterator = iterator;
        this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (iterator.hasNext() && !predicate.test(next = iterator.next())) next = null;
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

    public static class Sorted<T extends Comparable<T>> extends AbstractFunctionalIterator.Sorted<T> {

        private final FunctionalIterator.Sorted<T> source;
        private final Predicate<T> predicate;

        public Sorted(FunctionalIterator.Sorted<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public boolean hasNext() {
            while (source.hasNext() && !predicate.test(source.peek())) source.next();
            return source.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            return source.next();
        }

        @Override
        public T peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return source.peek();
        }

        @Override
        public void seek(T target) {
            this.source.seek(target);
        }

        @Override
        public void recycle() {
            source.recycle();
        }
    }
}

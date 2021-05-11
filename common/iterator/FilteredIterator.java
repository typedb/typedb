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
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

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

    public static class Sorted<T extends Comparable<? super T>> extends AbstractFunctionalIterator.Sorted<T> {

        private final FunctionalIterator.Sorted<T> source;
        private final Predicate<T> predicate;
        private T next;
        private T last;

        public Sorted(FunctionalIterator.Sorted<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public boolean hasNext() {
            return (next != null) || fetchAndCheck();
        }

        private boolean fetchAndCheck() {
            while (source.hasNext() && !predicate.test(next = source.next())) next = null;
            return next != null;
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
        public void seek(T target) {
            if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT); // cannot use backward seeks
            this.source.seek(target);
            this.next = null;
        }

        @Override
        public void recycle() {
            source.recycle();
        }
    }
}

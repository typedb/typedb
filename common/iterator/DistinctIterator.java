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

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

// TODO: verify (and potentially fix) this class is able to handle null objects
class DistinctIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Set<T> consumed;
    private T next;

    public DistinctIterator(FunctionalIterator<T> iterator) {
        this(iterator, new HashSet<>());
    }

    public DistinctIterator(FunctionalIterator<T> iterator, Set<T> duplicates) {
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

    public static class Sorted<T, K extends Comparable<K>> extends AbstractFunctionalIterator.Sorted<T, K> {

        private FunctionalIterator.Sorted<T, K> source;
        T last;

        public Sorted(AbstractFunctionalIterator.Sorted<T, K> source, Function<T, K> keyExtractor) {
            super(keyExtractor);
            this.source = source;
            last = null;
        }

        @Override
        public boolean hasNext() {
            while (source.hasNext()) {
                if (source.peek().equals(last)) source.next();
                else return true;
            }
            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            last = source.next();
            return last;
        }

        @Override
        public T peek() {
            if (!hasNext()) throw new NoSuchElementException();
            return source.peek();
        }

        @Override
        public void recycle() {
            source.recycle();
        }
    }
}

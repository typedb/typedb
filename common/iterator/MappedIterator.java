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


    /*
    UNSAFE
    The user must guarantee the mapping function preserves the sort order that the the source provides,
    in the new domain.
     */
    static class Sorted<T extends Comparable<? super T>, U extends Comparable<? super U>> extends AbstractFunctionalIterator.Sorted<U> {
        private final FunctionalIterator.Sorted<T> source;
        private final Function<T, U> mappingFn;
        private final Function<U, T> reverseMappingFn;
        private U last;

        public Sorted(FunctionalIterator.Sorted<T> source, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            this.source = source;
            this.mappingFn = mappingFn;
            this.reverseMappingFn = reverseMappingFn;
            last = null;
        }

        @Override
        public void seek(U target) {
            T reverseMapped = reverseMappingFn.apply(target);
            source.seek(reverseMapped);
        }

        @Override
        public U peek() {
            // TODO optimise
            return mappingFn.apply(source.peek());
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public U next() {
            if (!hasNext()) throw new NoSuchElementException();
            T next = source.next();
            U mappedNext = mappingFn.apply(next);
            assert last == null || mappedNext.compareTo(last) >= 0 : "Sorted mapped iterator produces out of order values";
            last = mappedNext;
            return mappedNext;
        }

        @Override
        public void recycle() {
            source.recycle();
        }
    }
}

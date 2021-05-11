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

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

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
        private State state;
        private U next;
        private U last;

        private enum State {EMPTY, FETCHED, COMPLETED};

        public Sorted(FunctionalIterator.Sorted<T> source, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            this.source = source;
            this.mappingFn = mappingFn;
            this.reverseMappingFn = reverseMappingFn;
            this.state = State.EMPTY;
            last = null;
        }

        @Override
        public void seek(U target) {
            if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT); // cannot use backward seeks
            T reverseMapped = reverseMappingFn.apply(target);
            source.seek(reverseMapped);
            state = State.EMPTY;
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
            if (source.hasNext()) {
                next = mappingFn.apply(source.next());
                state = State.FETCHED;
            } else {
                state = State.COMPLETED;
            }
            return state == State.FETCHED;
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
            source.recycle();
        }
    }
}

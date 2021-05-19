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

public class FinaliseHandledIterator<T> extends AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Runnable function;

    public FinaliseHandledIterator(FunctionalIterator<T> iterator, Runnable function) {
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

    public static class Sorted<T extends Comparable<T>> extends AbstractFunctionalIterator.Sorted<T> {

        private FunctionalIterator.Sorted<T> source;
        private Runnable function;

        public Sorted(FunctionalIterator.Sorted<T> source, Runnable function) {
            this.source = source;
            this.function = function;
        }

        @Override
        public void seek(T target) {
            source.seek(target);
        }

        @Override
        public T peek() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public T next() {
            return source.next();
        }

        @Override
        public void recycle() {
            source.recycle();
        }

        @Override
        protected void finalize() {
            function.run();
        }
    }
}

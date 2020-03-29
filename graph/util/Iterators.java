/*
 * Copyright (C) 2020 Grakn Labs
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

package hypergraph.graph.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class Iterators {

    public static <T> LoopIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    @SafeVarargs
    public static <T> LinkedIterators<T> link(Iterator<T>... iterators) {
        return new LinkedIterators<>(new LinkedList<>(Arrays.asList(iterators)));
    }

    public static <T> FilteredIterator<T> filter(Iterator<T> iterator, Predicate<T> predicate) {
        return new FilteredIterator<>(iterator, predicate);
    }

    public static <T, U> AppliedIterator<T, U> apply(Iterator<T> iterator, Function<T, U> function) {
        return new AppliedIterator<>(iterator, function);
    }

    public static abstract class BuilderIterator<T> implements Iterator<T> {

        public LinkedIterators<T> link(Iterator<T> iterator) {
            return new LinkedIterators<>(new LinkedList<>(Arrays.asList(this, iterator)));
        }

        public FilteredIterator<T> filter(Predicate<T> predicate) {
            return new FilteredIterator<>(this, predicate);
        }

        public <U> AppliedIterator<T, U> apply(Function<T, U> function) {
            return new AppliedIterator<>(this, function);
        }
    }

    public static class LoopIterator<T> extends BuilderIterator<T> {

        private final Predicate<T> predicate;
        private final UnaryOperator<T> function;
        private T next;
        private State state;
        private enum State {EMPTY, FETCHED, COMPLETED}

        LoopIterator(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
            state = State.FETCHED; // because first result is 'seed'
            this.next = seed;
            this.predicate = predicate;
            this.function = function;
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
                default: // This should never be reached
                    return false;
            }
        }

        private boolean fetchAndCheck() {
            next = function.apply(next);
            if (!predicate.test(next)) {
                state = State.COMPLETED;
                return false;
            }
            state = State.FETCHED;
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            state = State.EMPTY;
            return next;
        }
    }

    public static class LinkedIterators<T> extends BuilderIterator<T> {

        private final List<Iterator<T>> iterators;

        LinkedIterators(LinkedList<Iterator<T>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public final LinkedIterators<T> link(Iterator<T> iterator) {
            iterators.add(iterator);
            return this;
        }

        @Override
        public boolean hasNext() {
            while (!iterators.get(0).hasNext() && iterators.size() > 1) {
                iterators.remove(0);
            }
            return iterators.get(0).hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            return iterators.get(0).next();
        }
    }

    public static class FilteredIterator<T> extends BuilderIterator<T> {

        private final Iterator<T> iterator;
        private final Predicate<T> predicate;
        private T next;
        private boolean fetched;

        FilteredIterator(Iterator<T> iterator, Predicate<T> predicate) {
            this.iterator = iterator;
            this.predicate = predicate;
            fetched = false;
        }

        @Override
        public boolean hasNext() {
            return (next != null) || fetchAndCheck();
        }

        private boolean fetchAndCheck() {
            while (iterator.hasNext() && !predicate.test((next = iterator.next()))) next = null;
            return next != null;
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            T result = next;
            next = null;
            return result;
        }
    }

    public static class AppliedIterator<T, U> extends BuilderIterator<U> {

        private final Iterator<T> iterator;

        private final Function<T,U> function;

        AppliedIterator(Iterator<T> iterator, Function<T, U> function) {
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

    }
}

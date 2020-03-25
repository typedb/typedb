package hypergraph.graph.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class Iterators {

    public static <T, U> AppliedIterator<T, U> apply(Iterator<T> iterator, Function<T, U> function) {
        return new AppliedIterator<>(iterator, function);
    }

    @SafeVarargs
    public static <T> LinkedIterators<T> link(Iterator<T>... iterators) {
        return new LinkedIterators<>(new LinkedList<>(Arrays.asList(iterators)));
    }

    public static class LinkedIterators<T> implements Iterator<T> {

        private final List<Iterator<T>> iterators;

        LinkedIterators(LinkedList<Iterator<T>> iterators) {
            this.iterators = iterators;
        }

        public <U> AppliedIterator<T, U> apply(Function<T, U> function) {
            return new AppliedIterator<>(this, function);
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

    public static class AppliedIterator<T, U> implements Iterator<U> {

        private final Iterator<T> iterator;
        private final Function<T,U> function;

        private AppliedIterator(Iterator<T> iterator, Function<T, U> function) {
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

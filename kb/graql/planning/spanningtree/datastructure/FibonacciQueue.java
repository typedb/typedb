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

package grakn.core.kb.graql.planning.spanningtree.datastructure;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;

/**
 * A PriorityQueue built on top of a FibonacciHeap
 *
 * @param <E> the type of the values stored
 */
public class FibonacciQueue<E> extends AbstractQueue<E> {
    private final FibonacciHeap<E, E> heap;
    private final Function<FibonacciHeap<E, ?>.Entry, E> getValue = input -> {
        Preconditions.checkNotNull(input);
        return input.value;
    };

    private FibonacciQueue(FibonacciHeap<E, E> heap) {
        this.heap = heap;
    }

    public static <C> FibonacciQueue<C> create(Comparator<? super C> comparator) {
        return new FibonacciQueue<>(FibonacciHeap.create(comparator));
    }

    public static <C extends Comparable> FibonacciQueue<C> create() {
        return new FibonacciQueue<>(FibonacciHeap.create());
    }

    public Comparator<? super E> comparator() {
        return heap.comparator();
    }

    @Override
    public E peek() {
        return heap.peek().value;
    }

    @Override
    public boolean offer(E e) {
        return heap.add(e, e) != null;
    }

    @Override
    public E poll() {
        return heap.poll();
    }

    @Override
    public int size() {
        return heap.size();
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.transform(heap.iterator(), getValue::apply);
    }
}

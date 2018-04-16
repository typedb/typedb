/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.spanningtree.datastructure;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.google.common.collect.Iterators;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * A PriorityQueue built on top of a FibonacciHeap
 *
 * @param <E> the type of the values stored
 * @author Jason Liu
 */
public class FibonacciQueue<E> extends AbstractQueue<E> {
    private final FibonacciHeap<E, E> heap;
    private final Function<FibonacciHeap<E, ?>.Entry, E> getValue = input -> {
        assert input != null;
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
        Optional<FibonacciHeap<E, E>.Entry> first = heap.peekOption();
        return first.map(entry -> entry.value).orElse(null);
    }

    @Override
    public boolean offer(E e) {
        return heap.add(e, e).isPresent();
    }

    @Override
    public E poll() {
        return heap.pollOption().orElse(null);
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

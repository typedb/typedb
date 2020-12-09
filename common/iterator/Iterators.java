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

package grakn.core.common.iterator;

import grakn.common.collection.Either;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.common.collection.Collections.set;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public class Iterators {

    public static <T> ResourceIterator<T> empty() {
        return iterate(set());
    }

    public static <T> BaseIterator<T> single(T item) {
        return iterate(set(item));
    }

    public static <T> BaseIterator<T> iterate(Collection<T> collection) {
        return new BaseIterator<>(Either.second(collection.iterator()));
    }

    public static <T> BaseIterator<T> iterate(Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> LinkedIterators<T> link(List<? extends Iterator<T>> iterators) {
        final LinkedList<ResourceIterator<T>> converted = new LinkedList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof ResourceIterator<?>) {
                converted.addLast((ResourceIterator<T>) iterator);
            } else {
                converted.addLast(Iterators.iterate(iterator));
            }
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> LoopIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> TreeIterator<T> tree(T root, Function<T, ResourceIterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> ParallelIterators<T> parallel(List<ResourceIterator<T>> iterators) {
        return new ParallelIterators<>(iterators);
    }

    public static <T> ParallelIterators<T> parallel(List<ResourceIterator<T>> iterators, int bufferSize) {
        return new ParallelIterators<>(iterators, bufferSize);
    }

    public static <T> ParallelIterators<T> parallel(List<ResourceIterator<T>> iterators, int bufferSize, int bufferMultiplier) {
        return new ParallelIterators<>(iterators, bufferSize, bufferMultiplier);
    }

    public static <T> SynchronisedIterator<T> synchronised(ResourceIterator<T> iterator) {
        return new SynchronisedIterator<>(iterator);
    }

    public static <T> CartesianIterator<T> cartesian(List<ResourceIterator<T>> iteratorProducers) {
        return new CartesianIterator<>(iteratorProducers);
    }

    public static <T> PermutationIterator<T> permutation(List<T> list) {
        return new PermutationIterator<>(list);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }
}

/*
 * Copyright (C) 2021 Grakn Labs
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public class Iterators {

    public static <T> ResourceIterator<T> empty() {
        return iterate(set());
    }

    public static <T> ResourceIterator<T> single(T item) {
        return iterate(set(item));
    }

    public static <T> ResourceIterator<T> iterate(Collection<T> collection) {
        return new BaseIterator<>(Either.second(collection.iterator()));
    }

    public static <T> ResourceIterator<T> iterate(Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> ResourceIterator<T> link(Iterator<? extends T> iter1, Iterator<? extends T> iter2) {
        return link(list(iter1, iter2));
    }

    public static <T> ResourceIterator<T> link(Iterator<? extends T> iter1, Iterator<? extends T> iter2,
                                               Iterator<? extends T> iter3) {
        return link(list(iter1, iter2, iter3));
    }

    @SuppressWarnings("unchecked")
    public static <T> ResourceIterator<T> link(List<? extends Iterator<? extends T>> iterators) {
        List<ResourceIterator<T>> converted = new ArrayList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof AbstractResourceIterator<?>) {
                converted.add((ResourceIterator<T>) iterator);
            } else {
                converted.add(iterate((Iterator<T>) iterator));
            }
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> ResourceIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> ResourceIterator<T> tree(T root, Function<T, ResourceIterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> SynchronisedIterator<T> synchronised(ResourceIterator<T> iterator) {
        return new SynchronisedIterator<>(iterator);
    }

    public static <T> ResourceIterator<List<T>> cartesian(List<ResourceIterator<T>> iteratorProducers) {
        return new CartesianIterator<>(iteratorProducers);
    }

    public static <T> ResourceIterator<List<T>> permutation(Collection<T> list) {
        return new PermutationIterator<>(list);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }
}

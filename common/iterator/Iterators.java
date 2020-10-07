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

import static grakn.common.collection.Collections.list;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public class Iterators {

    public static <T> TreeIterator<T> tree(final T root, final Function<T, Iterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> LoopIterator<T> loop(final T seed, final Predicate<T> predicate, final UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> BaseIterator<T> iterate(final T item) {
        return new BaseIterator<>(Either.second(list(item).iterator()));
    }

    public static <T> BaseIterator<T> iterate(final Collection<T> collection) {
        return new BaseIterator<>(Either.second(collection.iterator()));
    }

    public static <T> BaseIterator<T> iterate(final Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> BaseIterator<T> iterate(final RecyclableIterator<T> iterator) {
        return new BaseIterator<>(Either.first(iterator));
    }

    public static <T> LinkedIterators<T> link(final RecyclableIterator<T> iterator1, final RecyclableIterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final RecyclableIterator<T> iterator1, final Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final Iterator<T> iterator1, final RecyclableIterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final Iterator<T> iterator1, final Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final List<Iterator<T>> iterators) {
        final LinkedList<Either<RecyclableIterator<T>, Iterator<T>>> converted = new LinkedList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof RecyclableIterator<?>)
                converted.addLast(Either.first((RecyclableIterator<T>) iterator));
            else converted.addLast(Either.second(iterator));
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> FilteredIterator<T> filter(final RecyclableIterator<T> iterator, final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.first(iterator), predicate);
    }

    public static <T> FilteredIterator<T> filter(final Iterator<T> iterator, final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.second(iterator), predicate);
    }

    public static <T, U> MappedIterator<T, U> apply(final RecyclableIterator<T> iterator, final Function<T, U> function) {
        return new MappedIterator<>(Either.first(iterator), function);
    }

    public static <T, U> MappedIterator<T, U> apply(final Iterator<T> iterator, final Function<T, U> function) {
        return new MappedIterator<>(Either.second(iterator), function);
    }

    public static <T> DistinctIterator<T> distinct(final RecyclableIterator<T> iterator) {
        return new DistinctIterator<>(Either.first(iterator));
    }

    public static <T> DistinctIterator<T> distinct(final Iterator<T> iterator) {
        return new DistinctIterator<>(Either.second(iterator));
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }
}

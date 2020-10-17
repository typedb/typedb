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

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public class Iterators {

    public static <T> TreeIterator<T> tree(final T root, final Function<T, ResourceIterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> LoopIterator<T> loop(final T seed, final Predicate<T> predicate, final UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> BaseIterator<T> iterate(final Collection<T> collection) {
        return new BaseIterator<>(Either.second(collection.iterator()));
    }

    public static <T> BaseIterator<T> iterate(final Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> LinkedIterators<T> link(final List<Iterator<T>> iterators) {
        final LinkedList<Either<ResourceIterator<T>, Iterator<T>>> converted = new LinkedList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof ResourceIterator<?>) {
                converted.addLast(Either.first((ResourceIterator<T>) iterator));
            } else {
                converted.addLast(Either.second(iterator));
            }
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> ParallelIterators<T> parallel(final List<ResourceIterator<T>> iterators) {
        return new ParallelIterators<>(iterators);
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }
}

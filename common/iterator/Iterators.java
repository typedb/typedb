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

    public static <T> BaseIterator<T> base(final Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> BaseIterator<T> base(final Recyclable<T> iterator) {
        return new BaseIterator<>(Either.first(iterator));
    }

    public static <T> LinkedIterators<T> link(final Recyclable<T> iterator1, final Recyclable<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final Recyclable<T> iterator1, final Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final Iterator<T> iterator1, final Recyclable<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final Iterator<T> iterator1, final Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(final List<Iterator<T>> iterators) {
        final LinkedList<Either<Recyclable<T>, Iterator<T>>> converted = new LinkedList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof Recyclable<?>) converted.addLast(Either.first((Recyclable<T>) iterator));
            else converted.addLast(Either.second(iterator));
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> FilteredIterator<T> filter(final Recyclable<T> iterator, final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.first(iterator), predicate);
    }

    public static <T> FilteredIterator<T> filter(final Iterator<T> iterator, final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.second(iterator), predicate);
    }

    public static <T, U> AppliedIterator<T, U> apply(final Recyclable<T> iterator, final Function<T, U> function) {
        return new AppliedIterator<>(Either.first(iterator), function);
    }

    public static <T, U> AppliedIterator<T, U> apply(final Iterator<T> iterator, final Function<T, U> function) {
        return new AppliedIterator<>(Either.second(iterator), function);
    }

    public static <T> DistinctIterator<T> distinct(final Recyclable<T> iterator) {
        return new DistinctIterator<>(Either.first(iterator));
    }

    public static <T> DistinctIterator<T> distinct(final Iterator<T> iterator) {
        return new DistinctIterator<>(Either.second(iterator));
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }

    public interface Recyclable<T> extends Iterator<T> {

        default Stream<T> stream() {
            return StreamSupport.stream(
                    spliteratorUnknownSize(this, ORDERED | IMMUTABLE), false
            ).onClose(this::recycle);
        }

        void recycle();
    }

    public interface Composable<T> extends Iterator<T> {

        default DistinctIterator<T> distinct() {
            return new DistinctIterator<>(Either.second(this));
        }

        default <U> AppliedIterator<T, U> apply(final Function<T, U> function) {
            return new AppliedIterator<>(Either.second(this), function);
        }

        default FilteredIterator<T> filter(final Predicate<T> predicate) {
            return new FilteredIterator<>(Either.second(this), predicate);
        }

        default LinkedIterators<T> link(final Iterators.Recyclable<T> iterator) {
            return new LinkedIterators<>(new LinkedList<>(list(Either.second(this), Either.first(iterator))));
        }

        default LinkedIterators<T> link(final Iterator<T> iterator) {
            if (iterator instanceof Iterators.Recyclable<?>) return link((Iterators.Recyclable<T>) iterator);
            return new LinkedIterators<>(new LinkedList<>(list(Either.second(this), Either.second(iterator))));
        }
    }
}

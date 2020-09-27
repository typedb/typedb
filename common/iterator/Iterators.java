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

    public static <T> TreeIterator<T> tree(T root, Function<T, Iterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> LoopIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> LinkedIterators<T> link(Recyclable<T> iterator1, Recyclable<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(Recyclable<T> iterator1, Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(Iterator<T> iterator1, Recyclable<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.first(iterator2))));
    }

    public static <T> LinkedIterators<T> link(Iterator<T> iterator1, Iterator<T> iterator2) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(iterator1), Either.second(iterator2))));
    }

    public static <T> LinkedIterators<T> link(List<Iterator<T>> iterators) {
        LinkedList<Either<Recyclable<T>, Iterator<T>>> converted = new LinkedList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof Recyclable<?>) converted.addLast(Either.first((Recyclable<T>) iterator));
            else converted.addLast(Either.second(iterator));
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> FilteredIterator<T> filter(Recyclable<T> iterator, Predicate<T> predicate) {
        return new FilteredIterator<>(Either.first(iterator), predicate);
    }

    public static <T> FilteredIterator<T> filter(Iterator<T> iterator, Predicate<T> predicate) {
        return new FilteredIterator<>(Either.second(iterator), predicate);
    }

    public static <T, U> AppliedIterator<T, U> apply(Recyclable<T> iterator, Function<T, U> function) {
        return new AppliedIterator<>(Either.first(iterator), function);
    }

    public static <T, U> AppliedIterator<T, U> apply(Iterator<T> iterator, Function<T, U> function) {
        return new AppliedIterator<>(Either.second(iterator), function);
    }

    public static <T> DistinctIterator<T> distinct(Recyclable<T> iterator) {
        return new DistinctIterator<>(Either.first(iterator));
    }

    public static <T> DistinctIterator<T> distinct(Iterator<T> iterator) {
        return new DistinctIterator<>(Either.second(iterator));
    }

    public static <T> Stream<T> stream(Recyclable<T> iterator) {
        return iterator.stream();
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
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

        default <U> AppliedIterator<T, U> apply(Function<T, U> function) {
            return new AppliedIterator<>(Either.second(this), function);
        }

        default FilteredIterator<T> filter(Predicate<T> predicate) {
            return new FilteredIterator<>(Either.second(this), predicate);
        }

        default LinkedIterators<T> link(Iterators.Recyclable<T> iterator) {
            return Iterators.link(this, iterator);
        }

        default LinkedIterators<T> link(Iterator<T> iterator) {
            if (iterator instanceof Iterators.Recyclable<?>) return link((Iterators.Recyclable<T>) iterator);
            return Iterators.link(this, iterator);
        }
    }

    public interface ComposableAndRecyclable<T> extends Composable<T>, Recyclable<T> {

        @Override
        default DistinctIterator<T> distinct() {
            return new DistinctIterator<>(Either.first(this));
        }

        @Override
        default <U> AppliedIterator<T, U> apply(Function<T, U> function) {
            return new AppliedIterator<>(Either.first(this), function);
        }

        @Override
        default FilteredIterator<T> filter(Predicate<T> predicate) {
            return new FilteredIterator<>(Either.first(this), predicate);
        }

        @Override
        default LinkedIterators<T> link(Iterators.Recyclable<T> iterator) {
            return Iterators.link(this, iterator);
        }

        @Override
        default LinkedIterators<T> link(Iterator<T> iterator) {
            if (iterator instanceof Iterators.Recyclable<?>) return link((Iterators.Recyclable<T>) iterator);
            return Iterators.link(this, iterator);
        }
    }
}

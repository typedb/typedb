/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SortedIterator<T extends Comparable<? super T>, ORDER extends SortedIterator.Order>
        extends FunctionalIterator<T> {

    Order.Asc ASC = new Order.Asc();
    Order.Desc DESC = new Order.Desc();

    abstract class Order {

        abstract <T extends Comparable<? super T>> int compare(T last, T next);

        abstract <T extends Comparable<? super T>> boolean isValidNext(T last, T next);

        // TODO: is it the right place to delegate these operations to?
        abstract <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source);

        abstract <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source, T from);

        public static class Asc extends Order {

            @Override
            <T extends Comparable<? super T>> int compare(T last, T next) {
                return last.compareTo(next);
            }

            @Override
            <T extends Comparable<? super T>> boolean isValidNext(T last, T next) {
                return last.compareTo(next) <= 0;
            }

            @Override
            <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source) {
                return source.iterator();
            }

            @Override
            <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source, T from) {
                return source.tailSet(from, true).iterator();
            }
        }

        public static class Desc extends Order {

            @Override
            <T extends Comparable<? super T>> int compare(T last, T next) {
                return -1 * last.compareTo(next);
            }

            @Override
            <T extends Comparable<? super T>> boolean isValidNext(T last, T next) {
                return last.compareTo(next) >= 0;
            }

            @Override
            <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source) {
                return source.descendingIterator();
            }

            @Override
            <T extends Comparable<? super T>> Iterator<T> iterateOrdered(NavigableSet<T> source, T from) {
                return source.headSet(from, true).descendingIterator();
            }
        }
    }

    ORDER order();

    T peek();

    SortedIterator<T, ORDER> merge(SortedIterator<T, ORDER> iterator);

    @Override
    SortedIterator<T, ORDER> distinct();

    @Override
    SortedIterator<T, ORDER> filter(Predicate<T> predicate);

    @Override
    SortedIterator<T, ORDER> limit(long limit);

    <U extends Comparable<? super U>, ORD extends Order> SortedIterator<U, ORD> mapSorted(Function<T, U> mappingFn, ORD order);

    NavigableSet<T> toNavigableSet();

    @Override
    SortedIterator<T, ORDER> onConsumed(Runnable function);

    @Override
    SortedIterator<T, ORDER> onFinalised(Runnable function);

    interface Seekable<T extends Comparable<? super T>, ORDER extends Order> extends SortedIterator<T, ORDER> {

        void seek(T target);

        Seekable<T, ORDER> merge(Seekable<T, ORDER> iterator);

        @Override
        Seekable<T, ORDER> distinct();

        @Override
        Seekable<T, ORDER> filter(Predicate<T> predicate);

        Seekable<T, ORDER> limit(long limit);

        <U extends Comparable<? super U>, ORD extends Order> Seekable<U, ORD> mapSorted(Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order);

        @Override
        Seekable<T, ORDER> onConsumed(Runnable function);

        @Override
        Seekable<T, ORDER> onFinalised(Runnable function);
    }
}

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
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SortedIterator<T extends Comparable<? super T>, ORDER extends SortedIterator.Order>
        extends FunctionalIterator<T> {

    Order.Asc ASC = new Order.Asc();
    Order.Desc DESC = new Order.Desc();

    interface Orderer {

        <T extends Comparable<? super T>> int compare(T last, T next);

        <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source);

        <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from);
    }

    abstract class Order {

        abstract Orderer orderer();

        <T extends Comparable<? super T>> boolean isValidNext(T last, T next) {
            return orderer().compare(last, next) <= 0;
        }

        public static class Asc extends Order {

            private static final Orderer orderer = new Orderer() {
                @Override
                public <T extends Comparable<? super T>> int compare(T last, T next) {
                    return last.compareTo(next);
                }

                @Override
                public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source) {
                    return source.iterator();
                }

                @Override
                public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from) {
                    return source.tailSet(from, true).iterator();
                }
            };

            @Override
            Orderer orderer() {
                return orderer;
            }
        }

        public static class Desc extends Order {

            private static final Orderer orderer = new Orderer() {

                @Override
                public <T extends Comparable<? super T>> int compare(T last, T next) {
                    return -1 * last.compareTo(next);
                }

                @Override
                public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source) {
                    return source.descendingIterator();
                }

                @Override
                public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from) {
                    return source.headSet(from, true).descendingIterator();
                }
            };

            @Override
            Orderer orderer() {
                return orderer;
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
    SortedIterator<T, ORDER> onFinalise(Runnable function);

    interface Forwardable<T extends Comparable<? super T>, ORDER extends Order> extends SortedIterator<T, ORDER> {

        void forward(T target);

        Forwardable<T, ORDER> merge(Forwardable<T, ORDER> iterator);

        @Override
        Forwardable<T, ORDER> distinct();

        @Override
        Forwardable<T, ORDER> filter(Predicate<T> predicate);

        Forwardable<T, ORDER> limit(long limit);

        default Optional<T> findFirst(T value) {
            if (!hasNext() || !order().isValidNext(peek(), value)) return Optional.empty();
            forward(value);
            Optional<T> found;
            if (hasNext() && peek().equals(value)) found = Optional.of(next());
            else found = Optional.empty();
            recycle();
            return found;
        }

        <U extends Comparable<? super U>, ORD extends Order> Forwardable<U, ORD> mapSorted(Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order);

        @Override
        Forwardable<T, ORDER> onConsumed(Runnable function);

        @Override
        Forwardable<T, ORDER> onFinalise(Runnable function);
    }
}

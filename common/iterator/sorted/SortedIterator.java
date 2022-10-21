/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SortedIterator<T extends Comparable<? super T>, ORDER extends Order>
        extends FunctionalIterator<T> {

    ORDER order();

    T peek();

    @Override
    SortedIterator<T, ORDER> distinct();

    @Override
    SortedIterator<T, ORDER> filter(Predicate<T> predicate);

    @Override
    SortedIterator<T, ORDER> limit(long limit);

    SortedIterator<T, ORDER> takeWhile(Function<T, Boolean> condition);

    NavigableSet<T> toNavigableSet();

    @Override
    SortedIterator<T, ORDER> onConsumed(Runnable function);

    @Override
    SortedIterator<T, ORDER> onFinalise(Runnable function);

    interface Forwardable<T extends Comparable<? super T>, ORDER extends Order> extends SortedIterator<T, ORDER> {

        void forward(T target);

        Forwardable<T, ORDER> merge(Forwardable<T, ORDER> iterator);

        Forwardable<T, ORDER> intersect(Forwardable<T, ORDER> iterator);

        @Override
        Forwardable<T, ORDER> distinct();

        @Override
        Forwardable<T, ORDER> filter(Predicate<T> predicate);

        Forwardable<T, ORDER> limit(long limit);

        Forwardable<T, ORDER> takeWhile(Function<T, Boolean> condition);

        <U extends Comparable<? super U>, ORD extends Order> Forwardable<U, ORD> mapSorted(Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order);

        @Override
        Forwardable<T, ORDER> onConsumed(Runnable function);

        @Override
        Forwardable<T, ORDER> onFinalise(Runnable function);

        default Optional<T> findFirst(T value) {
            if (!hasNext() || !order().inOrder(peek(), value)) return Optional.empty();
            forward(value);
            Optional<T> found;
            if (hasNext() && peek().equals(value)) found = Optional.of(next());
            else found = Optional.empty();
            recycle();
            return found;
        }
    }
}
